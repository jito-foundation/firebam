#define _GNU_SOURCE
#include "fd_event_client.c"
#include "../../util/tmpl/fd_unit_test.c"
#include <pthread.h>

/* Independent clocks and an acquisition fault only in this fixture.  The
   production recalibration/error handling and tile callbacks remain real. */
static long  test_mono;
static ulong test_mono_reads;
long fd_grpc_client_mono_now( void ) { test_mono_reads++; return test_mono; }
static int   test_recal_failure;
static int   test_virtual_wall;
static long  test_wall;
static ulong test_wall_reads;
static ulong test_recal_calls;
static long  test_wall_samples[4];
#include "../../util/clock/fd_clock.h"
static int
test_joint_read( fd_clock_func_t cx, void const * ax, fd_clock_func_t cy, void const * ay,
                 long * x, long * y, long * dx ) {
  test_recal_calls++;
  if( test_recal_failure ) {
    test_wall += 20000000L;
    test_mono += 1000000000L;
    return FD_CLOCK_ERR_Y;
  }
  return fd_clock_joint_read( cx, ax, cy, ay, x, y, dx );
}
#define fd_clock_joint_read test_joint_read
#define fd_clock_tile_now test_original_now
#include "../fd_clock_tile.h"
#undef fd_clock_joint_read
#undef fd_clock_tile_now
static inline long
fd_clock_tile_now( fd_clock_tile_t const * clock ) {
  long now = test_virtual_wall ? test_wall : test_original_now( clock );
  if( test_wall_reads<4UL ) test_wall_samples[test_wall_reads] = now;
  test_wall_reads++;
  return now;
}
#include "fd_event_tile.c"

typedef struct {
  fd_event_tile_t * tile;
  fd_event_client_t * client;
  fd_rng_t rng[1];
  fd_keyswitch_t keyswitch;
  ulong waker __attribute__((aligned(128)));
  void * queue_mem;
  int epoll;
  int peer;
} test_env_t;

static void
test_env_init( test_env_t * env, int connected ) {
  memset( env, 0, sizeof(*env) );
  env->peer = -1;
  env->epoll = epoll_create1( 0 ); FD_TEST( env->epoll>=0 );
  test_mono=10000000000L; test_virtual_wall=0; test_recal_failure=0;
  fd_rng_join( fd_rng_new( env->rng, 0U, 1UL ) );
  env->queue_mem=aligned_alloc(FD_CIRCQ_ALIGN,fd_ulong_align_up(fd_circq_footprint(512UL),FD_CIRCQ_ALIGN)); FD_TEST(env->queue_mem);
  fd_circq_t *q=fd_circq_join(fd_circq_new(env->queue_mem,512UL)); FD_TEST(q);
  void *mem=aligned_alloc(fd_event_client_align(),fd_event_client_footprint(65536UL)); FD_TEST(mem);
  uchar pubkey[32]={0};
  env->client=fd_event_client_join(fd_event_client_new(mem,NULL,env->rng,q,env->epoll,1<<20,
    "http://127.0.0.1:1",pubkey,"0.0.0","0000000000000000000000000000000000000000",
    "test",1UL,2UL,3UL,65536UL,0,NULL)); FD_TEST(env->client);
  env->client->has_genesis_hash=env->client->has_shred_version=1;
  env->client->consecutive_failure_count=0UL;
  env->tile=aligned_alloc(alignof(fd_event_tile_t),fd_ulong_align_up(sizeof(fd_event_tile_t),alignof(fd_event_tile_t))); FD_TEST(env->tile);
  memset(env->tile,0,sizeof(fd_event_tile_t));
  env->keyswitch.magic=FD_KEYSWITCH_MAGIC; env->keyswitch.state=FD_KEYSWITCH_STATE_COMPLETED;
  env->tile->client=env->client; env->tile->keyswitch=&env->keyswitch;
  env->tile->waker_fseq=&env->waker; env->tile->in_cnt=1UL;
  env->tile->next_poll_deadline=LONG_MAX;
  fd_clock_tile_init(env->tile->clock); fd_clock_tile_set(env->tile->clock,-10000000000L);
  env->tile->clock->shmem->recal_next=LONG_MAX;
  if(connected) {
    int sv[2]; FD_TEST(!socketpair(AF_UNIX,SOCK_STREAM|SOCK_NONBLOCK,0,sv));
    env->client->sockfd=sv[0]; env->peer=sv[1]; env->client->state=FD_EVENT_CLIENT_STATE_CONNECTED;
    struct epoll_event ev={.events=EPOLLIN,.data.fd=sv[0]}; FD_TEST(!epoll_ctl(env->epoll,EPOLL_CTL_ADD,sv[0],&ev));
    fd_grpc_client_t *grpc=env->client->grpc_client; grpc->h2_hs_done=1; grpc->conn->flags=0;
    env->client->event_stream=fd_grpc_client_stream_acquire(grpc,FD_EVENT_CLIENT_REQ_CTX_STREAM_EVENTS); FD_TEST(env->client->event_stream);
  }
}

static void
test_env_fini( test_env_t *env ) {
  if(env->client->sockfd>=0) FD_TEST(!close(env->client->sockfd));
  if(env->peer>=0) FD_TEST(!close(env->peer));
  FD_TEST(!close(env->epoll));
  free(env->client); free(env->queue_mem); free(env->tile);
  fd_rng_delete(fd_rng_leave(env->rng));
}

static void
test_partial_header( test_env_t *env ) {
  fd_h2_frame_hdr_t hdr={.typlen=fd_h2_frame_typlen(FD_H2_FRAME_TYPE_HEADERS,100UL),.r_stream_id=fd_uint_bswap(1U)};
  FD_TEST(write(env->peer,&hdr,sizeof(hdr))==(long)sizeof(hdr));
  int busy=0; FD_TEST(!fd_grpc_client_rxtx_socket(env->client->grpc_client,env->client->sockfd,-10000000000L,&busy));
  FD_TEST(env->client->grpc_client->has_block_deadline && env->client->grpc_client->conn->rx_hdrs_observed);
}

FD_UNIT_TEST( housekeeping_scope_matrix ) {
  for(uint scenario=0U;scenario<6U;scenario++) {
    test_env_t env[1];test_env_init(env,scenario!=0U);
    if(scenario>=2U) {test_partial_header(env);test_mono+=6000000000L;}
    if(scenario==3U) env->tile->clock->shmem->recal_next=LONG_MIN;
    if(scenario==4U) env->keyswitch.state=FD_KEYSWITCH_STATE_SWITCH_PENDING;
    if(scenario==5U) env->client->state=FD_EVENT_CLIENT_STATE_CONNECTING;
    ulong before=test_mono_reads;test_wall_reads=test_recal_calls=0UL;
    during_housekeeping(env->tile);
    FD_TEST(test_wall_reads==(scenario==3U ? 2UL : 1UL));
    FD_TEST(test_recal_calls==(scenario==3U ? 1UL : 0UL));
    if(scenario==2U || scenario==3U || scenario==5U) {
      FD_TEST(test_mono_reads>before && env->tile->next_poll_deadline==LONG_MIN);
      FD_TEST(env->client->grpc_client->conn->flags & FD_H2_CONN_FLAGS_DEAD);
      FD_TEST(env->client->defer_disconnect!=INT_MAX);
    } else {
      FD_TEST(test_mono_reads==before);
      if(scenario==0U || scenario==4U) FD_TEST(env->client->sockfd<0 && env->client->defer_disconnect==INT_MAX);
    }
    if(scenario==3U) FD_TEST(test_wall_samples[0]<0L && test_wall_samples[1]>1000000000000L && env->client->now==test_wall_samples[1]);
    FD_LOG_NOTICE(("Event HK scenario=%u wall_samples=%lu mono_reads=%lu",scenario,test_wall_reads,test_mono_reads-before));
    test_env_fini(env);
  }
}

FD_UNIT_TEST( negative_cleanup_fragment_and_future_zero ) {
  test_env_t env[1];test_env_init(env,1);test_partial_header(env);
  /* Full TX must not prevent local expiry/retirement. */
  fd_grpc_client_t *grpc=env->client->grpc_client;
  while(fd_h2_rbuf_free_sz(grpc->frame_tx)) fd_h2_rbuf_push(grpc->frame_tx,"",1UL);
  test_mono+=5000000000L;during_housekeeping(env->tile);
  FD_TEST(env->tile->next_poll_deadline==LONG_MIN && env->client->defer_disconnect!=INT_MAX);
  int busy=0;env->tile->idle_cnt=0UL;before_credit(env->tile,NULL,&busy);
  FD_TEST(env->client->sockfd>=0); /* finite idle throttle */
  env->tile->in_kind[0]=IN_KIND_IPECHO;
  after_frag(env->tile,0UL,0UL,1UL,0UL,0UL,0UL,NULL);
  FD_TEST(env->tile->next_poll_deadline==LONG_MIN);
  long t0=fd_tickcount(),due=next_deadline(env->tile),t1=fd_tickcount();FD_TEST(due>=t0 && due<=t1);
  before_credit(env->tile,NULL,&busy);
  FD_TEST(env->client->state==FD_EVENT_CLIENT_STATE_DISCONNECTED && env->client->sockfd<0 && env->client->defer_disconnect==INT_MAX);
  env->client->disconnected.reconnect_deadline=0L;
  env->tile->next_poll_deadline=fd_event_client_next_deadline(env->client,-10000000000L);
  FD_TEST(env->tile->next_poll_deadline==0L && next_deadline(env->tile)>fd_tickcount());
  env->tile->idle_cnt=1UL;before_credit(env->tile,NULL,&busy);
  FD_TEST(env->client->metrics.connect_attempt_cnt==0UL && env->client->sockfd<0);
  test_env_fini(env);
}

static fd_grpc_h2_stream_t *test_timed_stream;
static ulong test_timeout_count;
static long test_request_deadline;
static int test_reset_callback;
static void
test_timeout( void *ctx, ulong req, int kind ) {
  FD_TEST(req==77UL && kind==FD_GRPC_DEADLINE_RX_END);
  FD_TEST(test_timed_stream->has_rx_end_deadline && test_timed_stream->rx_end_deadline_nanos==test_request_deadline);
  test_timeout_count++;
  fd_event_client_grpc_rx_timeout(ctx,req,kind);
  if(test_reset_callback) {
    /* Exercise public reset plus the internal acquire callback seam.  Production
       Event callbacks normally defer retirement instead of resetting here. */
    fd_event_client_t *c=ctx;fd_grpc_client_reset(c->grpc_client);
    c->event_stream=fd_grpc_client_stream_acquire(c->grpc_client,FD_EVENT_CLIENT_REQ_CTX_STREAM_EVENTS);FD_TEST(c->event_stream);
    c->defer_disconnect=INT_MAX;
  }
}
FD_UNIT_TEST( failed_recal_request_composition ) {
  for(int full=0;full<2;full++) for(int reset=0;reset<2;reset++) {
    test_env_t env[1];test_env_init(env,1);test_partial_header(env);
    fd_grpc_client_t *grpc=env->client->grpc_client;
    ulong serial=grpc->conn->rx_hdrs_serial,generation=grpc->generation;long deadline=grpc->block_deadline_mono;
    test_timed_stream=fd_grpc_client_stream_acquire(grpc,77UL);FD_TEST(test_timed_stream);
    test_virtual_wall=1;test_wall=-10000000000L;test_request_deadline=test_wall+5000000L;
    fd_grpc_client_deadline_set(test_timed_stream,FD_GRPC_DEADLINE_RX_END,test_request_deadline);
    if(full) while(fd_h2_rbuf_free_sz(grpc->frame_tx)) fd_h2_rbuf_push(grpc->frame_tx,"",1UL);
    void (*old_timeout)(void *,ulong,int)=fd_event_client_grpc_callbacks.rx_timeout;
    fd_event_client_grpc_callbacks.rx_timeout=test_timeout;test_reset_callback=reset;
    long epoch=env->tile->clock->epoch->y0_eff;
    test_recal_failure=1;test_timeout_count=0UL;test_wall_reads=test_recal_calls=0UL;env->tile->clock->shmem->recal_next=LONG_MIN;
    during_housekeeping(env->tile);
    FD_TEST(test_recal_calls==1UL && test_wall_reads==2UL && test_timeout_count==1UL);
    FD_TEST(env->tile->clock->epoch->y0_eff==epoch && env->client->now==test_wall);
    FD_TEST(grpc->stream_cnt==1UL);
    if(reset) {
      FD_TEST(grpc->generation==generation+1UL && !grpc->has_block_deadline && !grpc->conn->rx_hdrs_observed);
      FD_TEST(!(grpc->conn->flags & FD_H2_CONN_FLAGS_DEAD) && env->client->event_stream);
      FD_TEST(env->client->defer_disconnect==INT_MAX && env->tile->next_poll_deadline==LONG_MAX);
    } else {
      FD_TEST(grpc->generation==generation);
      if(full) FD_TEST((grpc->conn->flags & FD_H2_CONN_FLAGS_DEAD) && !grpc->has_block_deadline);
      else FD_TEST(grpc->conn->rx_hdrs_serial==serial && grpc->has_block_deadline && grpc->block_deadline_mono==deadline && !(grpc->conn->flags & FD_H2_CONN_FLAGS_DEAD));
      FD_TEST(env->tile->next_poll_deadline==LONG_MIN && !env->client->event_stream);
      int busy=0;env->tile->idle_cnt=1UL;before_credit(env->tile,NULL,&busy);
      FD_TEST(env->client->sockfd<0 && env->client->defer_disconnect==INT_MAX);
    }
    FD_LOG_NOTICE(("Event failed recal full_tx=%d callback_reset=%d typed_timeout=1 original_deadline=1 fresh_wall=1",full,reset));
    fd_event_client_grpc_callbacks.rx_timeout=old_timeout;test_reset_callback=0;
    test_virtual_wall=test_recal_failure=0;test_env_fini(env);
  }
}

FD_UNIT_TEST( wrapper_retired_reset_and_saturated_projection ) {
  test_env_t env[1];test_env_init(env,1);test_partial_header(env);
  fd_grpc_client_t *grpc=env->client->grpc_client;
  fd_grpc_client_service_deadlines(grpc,LONG_MAX-10L);
  FD_TEST(fd_grpc_client_next_deadline(grpc)==LONG_MAX && grpc->has_block_deadline);
  test_mono+=5000000000L;ulong before=test_mono_reads;
  FD_TEST(fd_event_client_service_deadlines(env->client,-10000000000L));
  FD_TEST(test_mono_reads==before+1UL && (grpc->conn->flags & FD_H2_CONN_FLAGS_DEAD));
  /* Pending defer does not require another M read, even after block clear. */
  before=test_mono_reads;FD_TEST(fd_event_client_service_deadlines(env->client,0L));FD_TEST(test_mono_reads==before);
  disconnect(env->client,0L,DISCONNECT_REASON_IDENTITY_CHANGED,0,0);
  before=test_mono_reads;FD_TEST(!fd_event_client_service_deadlines(env->client,0L));FD_TEST(test_mono_reads==before);
  test_env_fini(env);
  test_env_init(env,1);test_partial_header(env);grpc=env->client->grpc_client;
  disconnect(env->client,0L,DISCONNECT_REASON_IDENTITY_CHANGED,0,0);
  FD_TEST(grpc->has_block_deadline && grpc->conn->rx_hdrs_observed);
  test_mono+=6000000000L;before=test_mono_reads;
  FD_TEST(!fd_event_client_service_deadlines(env->client,0L) && test_mono_reads==before);
  fd_grpc_client_reset(grpc);FD_TEST(!grpc->has_block_deadline && !grpc->conn->rx_hdrs_observed);
  before=test_mono_reads; /* reset itself takes its existing M sample */
  FD_TEST(!fd_event_client_service_deadlines(env->client,0L) && test_mono_reads==before);
  test_env_fini(env);
}

/* This peer drives real TCP/H2/TLS and Event protobuf authentication.  The
   signing worker uses the real keyguard transport/authorization and Ed25519,
   rather than invoking the complete sandboxed production sign tile. */
#include "../../waltz/tls/test_tls_helper.h"
#include "../../waltz/h2/fd_hpack.h"
#include "test_event_tls_cert.h"

typedef struct {
  fd_keyguard_client_t keyguard[1];
  fd_wksp_t * wksp;
  pthread_t thread;
  int stop;
  ulong signed_count;
  uchar private_key[32];
  uchar public_key[32];
} test_signer_t;

static void *
test_signer_run( void *arg ) {
  test_signer_t *s=arg;fd_keyguard_client_t *k=s->keyguard;
  fd_sha512_t sha[1];fd_sha512_join(fd_sha512_new(sha));
  fd_keyguard_authority_t authority[1];memset(authority,0,sizeof(authority));
  ulong seq=0UL;
  while(!__atomic_load_n(&s->stop,__ATOMIC_ACQUIRE)) {
    fd_frag_meta_t *m=&k->request[fd_mcache_line_idx(seq,k->request_depth)];
    if(fd_frag_meta_seq_query(m)!=seq) {sched_yield();continue;}
    FD_HW_MFENCE_LD();
    ulong sz=m->sz,chunk=m->chunk,sig=m->sig;
    FD_TEST(sz<=k->request_mtu && chunk>=k->request_chunk0 && chunk<=k->request_wmark);
    uchar data[512];memcpy(data,fd_chunk_to_laddr(k->request_mem,chunk),sz);
    FD_TEST(fd_keyguard_payload_authorize(authority,data,sz,FD_KEYGUARD_ROLE_EVENT,(int)sig));
    uchar signature[64];fd_ed25519_sign(signature,data,sz,s->public_key,s->private_key,sha);
    memcpy(fd_chunk_to_laddr(k->response_mem,k->response_chunk0),signature,64UL);
    /* Signing has completed before the response becomes consumable. */
    __atomic_store_n(&s->signed_count,seq+1UL,__ATOMIC_RELEASE);
    fd_mcache_publish(k->response,k->response_depth,seq,0UL,k->response_chunk0,64UL,0UL,0UL,0UL);
    seq++;
  }
  return NULL;
}

static void
test_signer_init( test_signer_t *s ) {
  memset(s,0,sizeof(*s));ulong pages=512UL,cpu=(ulong)sched_getcpu();
  s->wksp=fd_wksp_new_anon("event_test",4096UL,1UL,&pages,&cpu,0U,0UL);FD_TEST(s->wksp);
  ulong depth=8UL;
  fd_frag_meta_t *mc[2];uchar *dc[2];
  for(uint i=0U;i<2U;i++) {
    void *m=fd_wksp_alloc_laddr(s->wksp,fd_mcache_align(),fd_mcache_footprint(depth,0UL),1UL);FD_TEST(m);
    mc[i]=fd_mcache_join(fd_mcache_new(m,depth,0UL,0UL));FD_TEST(mc[i]);
    ulong data_sz=fd_dcache_req_data_sz(i ? 64UL : 512UL,depth,1UL,1);
    void *d=fd_wksp_alloc_laddr(s->wksp,fd_dcache_align(),fd_dcache_footprint(data_sz,0UL),1UL);FD_TEST(d);
    dc[i]=fd_dcache_join(fd_dcache_new(d,data_sz,0UL));FD_TEST(dc[i]);
  }
  FD_TEST(fd_keyguard_client_new(s->keyguard,mc[0],dc[0],mc[1],dc[1],512UL,64UL,NULL,ULONG_MAX,ULONG_MAX));
  for(uint i=0U;i<32U;i++) s->private_key[i]=(uchar)(i+40U);
  fd_sha512_t sha[1];fd_sha512_join(fd_sha512_new(sha));fd_ed25519_public_from_private(s->public_key,s->private_key,sha);
  FD_TEST(!pthread_create(&s->thread,NULL,test_signer_run,s));
}

static void
test_signer_fini( test_signer_t *s ) {
  __atomic_store_n(&s->stop,1,__ATOMIC_RELEASE);FD_TEST(!pthread_join(s->thread,NULL));fd_wksp_delete_anon(s->wksp);
}

typedef struct {
  int listener;
  int socket;
  ushort port;
  int use_tls;
  int withhold_ack;
  int preface;
  uint auth_stream;
  uint event_stream;
  ulong authenticated;
  uchar challenge[FD_EVENT_CLIENT_TOKEN_SZ];
  uchar identity[32];
  uchar rx[32768];ulong used;
  fd_tls_t tls[1];fd_tlsrec_conn_t tls_conn[1];
  fd_tls_test_sign_ctx_t sign[1];fd_chacha_rng_t random[1];
} test_server_t;

static void
test_send_all( int fd, void const *data, ulong sz ) {
  uchar const *p=data;long end=fd_log_wallclock()+2000000000L;
  while(sz) {
    long n=send(fd,p,sz,MSG_NOSIGNAL);
    if(n<0 && (errno==EAGAIN || errno==EINTR)) {FD_TEST(fd_log_wallclock()<end);sched_yield();continue;}
    FD_TEST(n>0);p+=n;sz-=(ulong)n;
  }
}

static void
test_server_send( test_server_t *s, void const *data, ulong sz ) {
  if(!s->use_tls) {test_send_all(s->socket,data,sz);return;}
  fd_tlsrec_slice_t app[1];fd_tlsrec_slice_init(app,(uchar *)data,sz);
  uchar cipher[FD_TLSREC_CAP];ulong used=sizeof(cipher);
  FD_TEST(fd_tlsrec_conn_tx(s->tls_conn,cipher,&used,app)==FD_TLSREC_SUCCESS);
  FD_TEST(fd_tlsrec_slice_is_empty(app));test_send_all(s->socket,cipher,used);
}

static void
test_server_frame( test_server_t *s, uint type, uint flags, uint id, void const *p, ulong n ) {
  uchar wire[1024];FD_TEST(n+sizeof(fd_h2_frame_hdr_t)<=sizeof(wire));
  fd_h2_frame_hdr_t h={.typlen=fd_h2_frame_typlen(type,n),.flags=(uchar)flags,.r_stream_id=fd_uint_bswap(id)};
  memcpy(wire,&h,sizeof(h));if(n) memcpy(wire+sizeof(h),p,n);
  test_server_send(s,wire,sizeof(h)+n);
}

static void
test_server_init( test_server_t *s, fd_rng_t *rng, int use_tls ) {
  memset(s,0,sizeof(*s));s->socket=-1;s->use_tls=use_tls;
  s->listener=socket(AF_INET,SOCK_STREAM|SOCK_NONBLOCK,0);FD_TEST(s->listener>=0);
  struct sockaddr_in addr={.sin_family=AF_INET,.sin_addr.s_addr=FD_IP4_ADDR(127,0,0,1)};
  FD_TEST(!bind(s->listener,fd_type_pun(&addr),sizeof(addr)));FD_TEST(!listen(s->listener,2));
  socklen_t len=sizeof(addr);FD_TEST(!getsockname(s->listener,fd_type_pun(&addr),&len));s->port=fd_ushort_bswap(addr.sin_port);
  if(use_tls) {
    fd_sha512_join(fd_sha512_new(s->sign->sha512));memcpy(s->sign->private_key,test_event_tls_private,32UL);
    fd_ed25519_public_from_private(s->sign->public_key,s->sign->private_key,s->sign->sha512);
    s->tls->rng=fd_tls_test_rand(s->random,rng);s->tls->sign=fd_tls_test_sign(s->sign);
    memcpy(s->tls->alpn,"\x02h2",3UL);s->tls->alpn_sz=3UL;
    memcpy(s->tls->cert_public_key,s->sign->public_key,32UL);
    memcpy(s->tls->cert_x509,test_event_tls_certificate,sizeof(test_event_tls_certificate));s->tls->cert_x509_sz=sizeof(test_event_tls_certificate);
    for(uint i=0U;i<32U;i++) s->tls->kex_private_key[i]=(uchar)fd_rng_uint(rng);
    fd_x25519_public(s->tls->kex_public_key,s->tls->kex_private_key);
  }
}

static void
test_server_headers( test_server_t *s, uint id, uchar const *data, ulong sz ) {
  fd_hpack_rd_t rd[1];fd_hpack_rd_init(rd,data,sz);uchar scratch[4096];
  while(!fd_hpack_rd_done(rd)) {
    uchar *p=scratch;fd_h2_hdr_t h[1];FD_TEST(!fd_hpack_rd_next(rd,h,&p,scratch+sizeof(scratch)));
    if(h->name_len==5U && !memcmp(h->name,":path",5UL)) {
      if(h->value_len==strlen("/events.v1.EventService/Authenticate") && !memcmp(h->value,"/events.v1.EventService/Authenticate",h->value_len)) s->auth_stream=id;
      else {FD_TEST(h->value_len==strlen("/events.v1.EventService/StreamEvents") && !memcmp(h->value,"/events.v1.EventService/StreamEvents",h->value_len));s->event_stream=id;}
    }
    if(h->name_len==13U && !memcmp(h->name,"authorization",13UL)) {
      FD_TEST(h->value_len==7UL+2UL*FD_EVENT_CLIENT_TOKEN_SZ+1UL+128UL && !memcmp(h->value,"Bearer ",7UL));
      uchar token[FD_EVENT_CLIENT_TOKEN_SZ],signature[64];
      FD_TEST(fd_hex_decode(token,h->value+7,FD_EVENT_CLIENT_TOKEN_SZ)==FD_EVENT_CLIENT_TOKEN_SZ);
      FD_TEST(!memcmp(token,s->challenge,sizeof(token)) && h->value[7UL+2UL*sizeof(token)]=='.');
      FD_TEST(fd_hex_decode(signature,h->value+7UL+2UL*sizeof(token)+1UL,64UL)==64UL);
      uchar payload[100UL+FD_EVENT_CLIENT_TOKEN_SZ];
      static char const prefix[100]="                                                                Firedancer event challenge-response";
      memcpy(payload,prefix,100UL);memcpy(payload+100,s->challenge,sizeof(token));
      fd_sha512_t sha[1];fd_sha512_join(fd_sha512_new(sha));
      FD_TEST(fd_ed25519_verify(payload,sizeof(payload),signature,s->identity,sha)==FD_ED25519_SUCCESS);
      s->authenticated++;
    }
  }
}

static void
test_server_step( test_server_t *s ) {
  if(s->socket<0) {
    s->socket=accept4(s->listener,NULL,NULL,SOCK_NONBLOCK);
    if(s->socket<0) {FD_TEST(errno==EAGAIN || errno==EINTR);return;}
    s->preface=0;s->used=0UL;s->auth_stream=s->event_stream=0U;
    if(s->use_tls) FD_TEST(fd_tlsrec_conn_init(s->tls_conn,s->tls,1));
  }
  uchar data[FD_TLSREC_CAP];long n=recv(s->socket,data,sizeof(data),0);
  if(n<0) {FD_TEST(errno==EAGAIN || errno==EINTR);return;}
  if(!n) return;
  if(s->use_tls) {
    fd_tlsrec_slice_t rx[1];fd_tlsrec_slice_init(rx,data,(ulong)n);
    uchar cipher[FD_TLSREC_CAP],app[FD_TLSREC_CAP];ulong cipher_sz=sizeof(cipher),app_sz=sizeof(app);
    FD_TEST(fd_tlsrec_conn_rx(s->tls_conn,rx,cipher,&cipher_sz,app,&app_sz)==FD_TLSREC_SUCCESS);
    FD_TEST(fd_tlsrec_slice_is_empty(rx));if(cipher_sz) test_send_all(s->socket,cipher,cipher_sz);
    FD_TEST(s->used+app_sz<=sizeof(s->rx));memcpy(s->rx+s->used,app,app_sz);s->used+=app_sz;
  } else {FD_TEST(s->used+(ulong)n<=sizeof(s->rx));memcpy(s->rx+s->used,data,(ulong)n);s->used+=(ulong)n;}
  ulong off=0UL;
  if(!s->preface) {if(s->used<24UL) return;FD_TEST(!memcmp(s->rx,"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n",24UL));off=24UL;s->preface=1;}
  while(s->used-off>=sizeof(fd_h2_frame_hdr_t)) {
    fd_h2_frame_hdr_t h;memcpy(&h,s->rx+off,sizeof(h));ulong len=fd_h2_frame_length(h.typlen);uint id=fd_uint_bswap(h.r_stream_id);
    if(s->used-off<sizeof(h)+len) break;
    uchar *p=s->rx+off+sizeof(h);uint type=fd_h2_frame_type(h.typlen);
    if(type==FD_H2_FRAME_TYPE_SETTINGS && !(h.flags & FD_H2_FLAG_ACK)) {
      test_server_frame(s,FD_H2_FRAME_TYPE_SETTINGS,0U,0U,NULL,0UL);
      if(!s->withhold_ack) test_server_frame(s,FD_H2_FRAME_TYPE_SETTINGS,FD_H2_FLAG_ACK,0U,NULL,0UL);
    } else if(type==FD_H2_FRAME_TYPE_HEADERS) {
      FD_TEST(h.flags & FD_H2_FLAG_END_HEADERS);test_server_headers(s,id,p,len);
      if(id==s->event_stream) {
        static uchar const headers[]="\x88\x5f\x10" "application/grpc";
        test_server_frame(s,FD_H2_FRAME_TYPE_HEADERS,FD_H2_FLAG_END_HEADERS,id,headers,sizeof(headers)-1UL);
        /* A heartbeat ACK has no circq cursor; ULONG_MAX is the wire sentinel. */
        uchar ack[]={0U,0U,0U,0U,11U,8U,255U,255U,255U,255U,255U,255U,255U,255U,255U,1U};test_server_frame(s,FD_H2_FRAME_TYPE_DATA,0U,id,ack,sizeof(ack));
      }
    } else if(type==FD_H2_FRAME_TYPE_DATA && id==s->auth_stream) {
      FD_TEST(len>=5UL && !p[0]);
      fd_pb_inbuf_t in[1];fd_pb_inbuf_init(in,p+5,len-5UL);fd_pb_tlv_t field;
      FD_TEST(fd_pb_read_tlv(in,&field) && field.field_id==1U && field.len==32UL && fd_pb_inbuf_sz(in)>=32UL);memcpy(s->identity,in->cur,32UL);
      static uchar const headers[]="\x88\x5f\x10" "application/grpc";
      static uchar const trailers[]="\x00\x0bgrpc-status\x01" "0";
      test_server_frame(s,FD_H2_FRAME_TYPE_HEADERS,FD_H2_FLAG_END_HEADERS,id,headers,sizeof(headers)-1UL);
      uchar body[5UL+3UL+FD_EVENT_CLIENT_TOKEN_SZ]={0U,0U,0U,0U,220U,0x0a,0xd9,0x01};memcpy(body+8,s->challenge,sizeof(s->challenge));
      test_server_frame(s,FD_H2_FRAME_TYPE_DATA,0U,id,body,sizeof(body));
      test_server_frame(s,FD_H2_FRAME_TYPE_HEADERS,FD_H2_FLAG_END_HEADERS|FD_H2_FLAG_END_STREAM,id,trailers,sizeof(trailers)-1UL);
    } else if(type==FD_H2_FRAME_TYPE_PING && !(h.flags & FD_H2_FLAG_ACK)) test_server_frame(s,type,FD_H2_FLAG_ACK,0U,p,len);
    off+=sizeof(h)+len;
  }
  memmove(s->rx,s->rx+off,s->used-off);s->used-=off;
}

static void
test_authenticate_until_ready( test_server_t *server, test_env_t *env ) {
  ulong ack_before=env->client->metrics.events_acked;long limit=fd_log_wallclock()+5000000000L;
  while(!env->client->event_stream || env->client->metrics.events_acked==ack_before) {
    FD_TEST(fd_log_wallclock()<limit);int busy=0;
    fd_event_client_poll(env->client,fd_log_wallclock(),&busy);test_server_step(server);sched_yield();
  }
  FD_TEST(env->client->state==FD_EVENT_CLIENT_STATE_CONNECTED && env->client->defer_disconnect==INT_MAX);
}

FD_UNIT_TEST( connecting_withheld_settings_ack ) {
  test_env_t env[1];test_env_init(env,0);
  test_server_t *server=aligned_alloc(alignof(test_server_t),fd_ulong_align_up(sizeof(test_server_t),alignof(test_server_t)));FD_TEST(server);
  test_server_init(server,env->rng,0);server->withhold_ack=1;env->client->server_tcp_port=server->port;
  long limit=fd_log_wallclock()+5000000000L;
  while(!server->preface) {
    FD_TEST(fd_log_wallclock()<limit);int busy=0;fd_event_client_poll(env->client,fd_log_wallclock(),&busy);test_server_step(server);
  }
  fd_h2_frame_hdr_t partial={.typlen=fd_h2_frame_typlen(FD_H2_FRAME_TYPE_HEADERS,100UL),.r_stream_id=fd_uint_bswap(1U)};
  test_server_send(server,&partial,sizeof(partial));
  while(!env->client->grpc_client->has_block_deadline) {
    FD_TEST(fd_log_wallclock()<limit);int busy=0;fd_event_client_poll(env->client,fd_log_wallclock(),&busy);
  }
  FD_TEST(env->client->state==FD_EVENT_CLIENT_STATE_CONNECTING && !env->client->grpc_client->h2_hs_done);
  test_mono+=5000000000L;during_housekeeping(env->tile);FD_TEST(env->tile->next_poll_deadline==LONG_MIN);
  int busy=0;env->tile->idle_cnt=1UL;before_credit(env->tile,NULL,&busy);
  FD_TEST(env->client->sockfd<0 && env->client->defer_disconnect==INT_MAX && !env->client->metrics.transport_success_cnt);
  FD_TEST(!close(server->socket) && !close(server->listener));free(server);test_env_fini(env);
  FD_LOG_NOTICE(("Event real CONNECTING withheld_SETTINGS_ACK partial_block_expired=1 retired=1"));
}

FD_UNIT_TEST( authenticated_reconnect_plain_and_tls ) {
  alarm(30U); /* Fail closed if the dedicated signer or peer stops progressing. */
  for(int tls=0;tls<2;tls++) {
    test_env_t env[1];test_env_init(env,0);
    test_signer_t signer[1];test_signer_init(signer);env->client->keyguard_client=signer->keyguard;
    memcpy(env->client->identity_pubkey,signer->public_key,32UL);
    test_server_t *server=aligned_alloc(alignof(test_server_t),fd_ulong_align_up(sizeof(test_server_t),alignof(test_server_t)));FD_TEST(server);test_server_init(server,env->rng,tls);
    for(uint i=0U;i<sizeof(server->challenge);i++) server->challenge[i]=(uchar)i;
    env->client->server_tcp_port=server->port;
    if(tls) {
      fd_x509_ca_store_t *ca=calloc(1,sizeof(fd_x509_ca_store_t));FD_TEST(ca);
      fd_x509_cert_info_t info;FD_TEST(!fd_x509_cert_parse(test_event_tls_certificate,sizeof(test_event_tls_certificate),&info));
      ca->cnt=1UL;memcpy(ca->entries[0].subject,info.subject,info.subject_len);ca->entries[0].subject_len=info.subject_len;
      memcpy(ca->entries[0].pubkey,info.pubkey,info.pubkey_len);ca->entries[0].pubkey_len=info.pubkey_len;ca->entries[0].key_type=info.key_type;
      env->client->use_tls=1;tls_init(env->client,ca);
      /* fd_tls's test server requests a client certificate unconditionally.
         This fixture therefore uses mutual TLS; ordinary Event peers use
         server-only TLS.  Do not bypass server CA/SAN validation. */
      env->client->tls->sign=fd_tls_test_sign(server->sign);
      memcpy(env->client->tls->cert_public_key,server->sign->public_key,32UL);
      memcpy(env->client->tls->cert_x509,test_event_tls_certificate,sizeof(test_event_tls_certificate));
      env->client->tls->cert_x509_sz=sizeof(test_event_tls_certificate);
    }
    test_authenticate_until_ready(server,env);
    FD_TEST(server->authenticated==1UL && env->client->metrics.transport_success_cnt==1UL);
    if(tls) FD_TEST(env->client->tls->ca_store && fd_tlsrec_conn_is_ready(env->client->tls_conn) && env->client->tls_conn->hs.cli.alpn_negotiated && !env->client->tls_conn->hs.cli.cert_verify_err);
    /* A real connected, signed stream now receives an incomplete trailer;
       expire only M and require actual HK + ordinary poll retirement. */
    uint id=env->client->event_stream->s.stream_id;
    fd_h2_frame_hdr_t partial={.typlen=fd_h2_frame_typlen(FD_H2_FRAME_TYPE_HEADERS,100UL),.r_stream_id=fd_uint_bswap(id),.flags=FD_H2_FLAG_END_STREAM};
    test_server_send(server,&partial,sizeof(partial));
    long limit=fd_log_wallclock()+2000000000L;
    while(!env->client->grpc_client->has_block_deadline) {
      FD_TEST(fd_log_wallclock()<limit);int busy=0;fd_event_client_poll(env->client,fd_log_wallclock(),&busy);
    }
    test_mono+=5000000000L;fd_clock_tile_set(env->tile->clock,-10000000000L);env->tile->clock->shmem->recal_next=LONG_MAX;
    during_housekeeping(env->tile);FD_TEST(env->tile->next_poll_deadline==LONG_MIN);
    int busy=0;env->tile->idle_cnt=1UL;before_credit(env->tile,NULL,&busy);
    FD_TEST(env->client->sockfd<0 && env->client->defer_disconnect==INT_MAX);
    FD_TEST(!close(server->socket));server->socket=-1;server->used=0UL;
    for(uint i=0U;i<sizeof(server->challenge);i++) server->challenge[i]^=0x5a;
    test_authenticate_until_ready(server,env);
    FD_TEST(server->authenticated==2UL && env->client->metrics.transport_success_cnt==2UL);
    if(tls) FD_TEST(fd_tlsrec_conn_is_ready(env->client->tls_conn) && env->client->tls_conn->hs.cli.alpn_negotiated && !env->client->tls_conn->hs.cli.cert_verify_err);
    FD_TEST(__atomic_load_n(&signer->signed_count,__ATOMIC_ACQUIRE)==2UL);
    FD_LOG_NOTICE(("Event integrated reconnect tls=%d mutual_tls=%d authenticated=2 actual_signatures=2 response_after_reconnect=1",tls,tls));
    if(tls) free((void *)env->client->tls->ca_store);
    FD_TEST(!close(server->socket) && !close(server->listener));free(server);test_signer_fini(signer);test_env_fini(env);
  }
  alarm(0U);
}

int main( int argc, char **argv ) {
  fd_boot(&argc,&argv);
  fd_unit_tests(argc,argv);
  FD_LOG_NOTICE(("pass"));
  fd_halt();return 0;
}
