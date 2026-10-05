#include "fd_grpc_codec.h"
#include "../h2/fd_hpack.h"
#include "../h2/fd_hpack_wr.h"

#include <limits.h>

static int
fd_hpack_wr_content_type_grpc( fd_h2_rbuf_t * rbuf_tx ) {
  static char const code[] =
    "\x5f" "\x16" "application/grpc+proto";
  if( FD_UNLIKELY( fd_h2_rbuf_free_sz( rbuf_tx)<sizeof(code)-1 ) ) return 0;
  fd_h2_rbuf_push( rbuf_tx, code, sizeof(code)-1 );
  return 1;
}

int
fd_grpc_h2_gen_request_hdrs( fd_grpc_req_hdrs_t const * req,
                             fd_h2_rbuf_t *             rbuf_tx,
                             char const *               version,
                             ulong                      version_len ) {
  if( FD_UNLIKELY( !fd_hpack_wr_method_post( rbuf_tx ) ) ) return 0;
  if( FD_UNLIKELY( !fd_hpack_wr_scheme( rbuf_tx, 1 ) ) ) return 0;
  if( FD_UNLIKELY( !fd_hpack_wr_path( rbuf_tx, req->path, req->path_len ) ) ) return 0;
  if( req->host_len ) {
    if( FD_UNLIKELY( !fd_hpack_wr_authority( rbuf_tx, req->host, req->host_len, req->port ) ) ) return 0;
  }
  if( FD_UNLIKELY( !fd_hpack_wr_trailers( rbuf_tx ) ) ) return 0;
  if( FD_UNLIKELY( !fd_hpack_wr_content_type_grpc( rbuf_tx ) ) ) return 0;

  static char const user_agent[] = "grpc-firedancer/";
  ulong const user_agent_len = sizeof(user_agent)-1 + version_len;
  if( FD_UNLIKELY( !fd_hpack_wr_user_agent( rbuf_tx, user_agent_len ) ) ) return 0;
  fd_h2_rbuf_push( rbuf_tx, user_agent, sizeof(user_agent)-1 );
  fd_h2_rbuf_push( rbuf_tx, version,    version_len          );

  if( req->bearer_auth_len ) {
    if( FD_UNLIKELY( !fd_hpack_wr_auth_bearer( rbuf_tx, req->bearer_auth, req->bearer_auth_len ) ) ) return 0;
  }
  return 1;
}

/* fd_grpc_h2_parse_num parses a strictly-decimal unsigned integer from
   a header value.  Every byte must be in ['0','9'].  Returns the parsed
   value, or UINT_MAX on failure (empty, any non-digit character
   including whitespace/sign/hex prefix, or overflow). */

static uint
fd_grpc_h2_parse_num( char const * num,
                      ulong        num_len ) {
  if( FD_UNLIKELY( !num_len || num_len>10UL ) ) return UINT_MAX;
  ulong val = 0;
  for( ulong i=0; i<num_len; i++ ) {
    uint d = (uint)( (uchar)num[i] - '0' );
    if( FD_UNLIKELY( d>9U ) ) return UINT_MAX;
    val = val*10UL + d;
  }
  if( FD_UNLIKELY( val>(ulong)UINT_MAX ) ) return UINT_MAX;
  return (uint)val;
}

/* HTTP/2 requires lowercase names and forbids delimiter/control bytes.
   Validate the full HTTP token grammar, including unknown metadata.  The
   only response pseudo-header is :status; its placement is checked below. */
static int
fd_grpc_h2_name_valid( fd_h2_hdr_t const * hdr ) {
  if( hdr->name_len==7U && fd_memeq( hdr->name, ":status", 7UL ) ) return 1;
  if( !hdr->name_len ) return 0;
  for( ulong i=0UL; i<hdr->name_len; i++ ) {
    uchar c = (uchar)hdr->name[i];
    if( (c>='a' && c<='z') || (c>='0' && c<='9') ) continue;
    switch( c ) {
    case '!': case '#': case '$': case '%': case '&': case '\'':
    case '*': case '+': case '-': case '.': case '^': case '_': case '`': case '|': case '~': continue;
    default: return 0;
    }
  }
  return 1;
}

static int
fd_grpc_h2_value_valid( fd_h2_hdr_t const * hdr ) {
  if( !hdr->value_len ) return 1;
  uchar first = (uchar)hdr->value[0];
  uchar last  = (uchar)hdr->value[hdr->value_len-1U];
  if( first==' ' || first=='\t' || last==' ' || last=='\t' ) return 0;
  /* Interior SP/HTAB and obs-text are valid.  Accumulation permits compiler
     vectorization while reading exactly the validated decoded length. */
  uchar invalid = 0U;
  for( ulong i=0UL; i<hdr->value_len; i++ ) {
    uchar c = (uchar)hdr->value[i];
    invalid |= (uchar)(((c<32U) & (c!=9U)) | (c==127U));
  }
  return !invalid;
}

static int
fd_grpc_h2_connection_field( fd_h2_hdr_t const * hdr ) {
#define CONNECTION_FIELD(s) (hdr->name_len==sizeof(s)-1UL && fd_memeq( hdr->name, s, sizeof(s)-1UL ))
  int prohibited = CONNECTION_FIELD("connection") || CONNECTION_FIELD("proxy-connection") ||
                   CONNECTION_FIELD("keep-alive") || CONNECTION_FIELD("transfer-encoding") ||
                   CONNECTION_FIELD("upgrade") || CONNECTION_FIELD("te");
#undef CONNECTION_FIELD
  return prohibited; /* TE is allowed only in requests, never responses. */
}

/* Match tonic's Code::from_bytes, retaining the existing UNKNOWN diagnostic
   through UINT_MAX.  A general decimal parser would accept leading zeroes. */
static uint
fd_grpc_h2_parse_status( char const * value, ulong len ) {
  if( len==1UL && value[0]>='0' && value[0]<='9' ) return (uint)(value[0]-'0');
  if( len==2UL && value[0]=='1' && value[1]>='0' && value[1]<='6' ) return 10U+(uint)(value[1]-'0');
  return UINT_MAX;
}

int
fd_grpc_h2_read_response_hdrs_ex( fd_grpc_resp_hdrs_t *       resp,
                               fd_h2_hdr_matcher_t const * matcher,
                               uchar const *               payload,
                               ulong                       payload_sz,
                               uchar *                     scratch_buf,
                               ulong                       scratch_sz,
                               int                         trailers,
                               uint *                      status_cnt ) {
  int semantic_err = 0;
  int observed_regular = 0;
  int observed_content_type = 0;
  int observed_grpc_status = 0;
  int warned = 0;
  ulong decoded_sz = 0UL;
  *status_cnt = 0U;
  fd_hpack_rd_t hpack_rd[1];
  fd_hpack_rd_init( hpack_rd, payload, payload_sz );
  while( !fd_hpack_rd_done( hpack_rd ) )  {
    uchar * scratch = scratch_buf;
    fd_h2_hdr_t hdr[1];
    uint err = fd_hpack_rd_next( hpack_rd, hdr, &scratch, scratch_buf+scratch_sz );
    if( FD_UNLIKELY( err ) ) {
      FD_LOG_WARNING(( "Failed to parse response headers (%u-%s)", err, fd_h2_strerror( err ) ));
      return (int)err;
    }
    ulong field_sz = (ulong)hdr->name_len + (ulong)hdr->value_len + 32UL;
    if( FD_UNLIKELY( field_sz>FD_GRPC_HEADER_LIST_MAX-decoded_sz ) ) return FD_H2_ERR_ENHANCE_YOUR_CALM;
    decoded_sz += field_sz;
    /* Count exact decoded :status even when its value is invalid.  Invalid
       bytes must not reach the matcher or status-value diagnostics. */
    if( hdr->name_len==7U && hdr->name[0]==':' && fd_memeq( hdr->name, ":status", 7UL ) ) (*status_cnt)++;
    if( FD_UNLIKELY( !fd_grpc_h2_name_valid( hdr ) || !fd_grpc_h2_value_valid( hdr ) ||
                     fd_grpc_h2_connection_field( hdr ) ) ) {
      semantic_err = 1;
      continue; /* Complete HPACK validation still takes precedence. */
    }
    if( hdr->name_len && hdr->name[0]==':' ) {
      if( observed_regular || trailers || hdr->name_len!=7U || !fd_memeq(hdr->name, ":status", 7UL) ) semantic_err = 1;
    } else observed_regular = 1;

    int hdr_idx = fd_h2_hdr_match( matcher, hdr->name, hdr->name_len, hdr->hint );
    switch( hdr_idx ) {
    case FD_H2_HDR_STATUS: {
      if( FD_UNLIKELY( hdr->value_len!=3 ) ) {
        if( !warned++ ) FD_LOG_WARNING(( "Invalid HTTP status length %u", hdr->value_len ));
        semantic_err = 1;
        break;
      }
      uint h2_status = fd_grpc_h2_parse_num( hdr->value, hdr->value_len );
      /* [100,999] matches the http crate's StatusCode::from_bytes used
         by tonic/h2 (first digit 1-9). RFC 9110 only defines 100-599
         but the h2 crate accepts the full 3-digit range. */
      if( FD_UNLIKELY( h2_status<100U || h2_status>999U ) ) {
        if( !warned++ ) FD_LOG_WARNING(( "Invalid HTTP status %u", h2_status ));
        semantic_err = 1;
        break;
      }
      resp->h2_status = h2_status;
      break;
    }
    case FD_H2_HDR_CONTENT_TYPE:
      if( observed_content_type++ ) semantic_err = 1;
      if( trailers ) break; /* preserve the established response media type */
      resp->is_grpc_proto =
        ( ( hdr->value_len==(sizeof("application/grpc")-1UL) &&
            fd_memeq( hdr->value, "application/grpc", sizeof("application/grpc")-1UL ) ) ||
          ( hdr->value_len==(sizeof("application/grpc+proto")-1UL) &&
            fd_memeq( hdr->value, "application/grpc+proto", sizeof("application/grpc+proto")-1UL ) ) );
      break;
    case FD_GRPC_HDR_STATUS: {
      if( observed_grpc_status++ ) semantic_err = 1;
      uint grpc_status = fd_grpc_h2_parse_status( hdr->value, hdr->value_len );
      /* Tonic's Code::from_bytes maps any unrecognized grpc-status
         (including leading zeroes, >16, non-numeric, empty) to Code::Unknown rather
         than rejecting the stream. We match that behavior here so that
         in all cases, we don't cause spurious RST_STREAMs. */
      if( FD_UNLIKELY( grpc_status>FD_GRPC_STATUS_UNAUTHENTICATED ) ) {
        int trunc_len = (int)fd_ulong_min( hdr->value_len, 32UL );
        if( !warned++ ) FD_LOG_WARNING(( "Unknown grpc-status \"%.*s\", treating as UNKNOWN", trunc_len, hdr->value ));
        grpc_status = FD_GRPC_STATUS_UNKNOWN;
      }
      resp->grpc_status = grpc_status;
      break;
    }
    case FD_GRPC_HDR_MESSAGE:
      resp->grpc_msg_len = (uint)fd_ulong_min( hdr->value_len, sizeof(resp->grpc_msg) );
      if( resp->grpc_msg_len ) {
        fd_memcpy( resp->grpc_msg, hdr->value, resp->grpc_msg_len );
      }
      break;
    }
  }
  return semantic_err ? FD_H2_ERR_PROTOCOL : FD_H2_SUCCESS;
}

int
fd_grpc_h2_read_response_hdrs( fd_grpc_resp_hdrs_t *       resp,
                               fd_h2_hdr_matcher_t const * matcher,
                               uchar const *              payload,
                               ulong                      payload_sz ) {
  uchar scratch[ 2UL*FD_GRPC_HEADER_LIST_MAX ];
  uint status_cnt;
  return fd_grpc_h2_read_response_hdrs_ex( resp, matcher, payload, payload_sz,
                                          scratch, sizeof(scratch), 0, &status_cnt );
}

char const *
fd_grpc_status_cstr( uint status ) {
  switch( status ) {
  case FD_GRPC_STATUS_OK:                   return "ok";
  case FD_GRPC_STATUS_CANCELLED:            return "cancelled";
  case FD_GRPC_STATUS_UNKNOWN:              return "unknown";
  case FD_GRPC_STATUS_INVALID_ARGUMENT:     return "invalid argument";
  case FD_GRPC_STATUS_DEADLINE_EXCEEDED:    return "deadline exceeded";
  case FD_GRPC_STATUS_NOT_FOUND:            return "not found";
  case FD_GRPC_STATUS_ALREADY_EXISTS:       return "already exists";
  case FD_GRPC_STATUS_PERMISSION_DENIED:    return "permission denied";
  case FD_GRPC_STATUS_RESOURCE_EXHAUSTED:   return "resource exhausted";
  case FD_GRPC_STATUS_FAILED_PRECONDITION:  return "failed precondition";
  case FD_GRPC_STATUS_ABORTED:              return "aborted";
  case FD_GRPC_STATUS_OUT_OF_RANGE:         return "out of range";
  case FD_GRPC_STATUS_UNIMPLEMENTED:        return "unimplemented";
  case FD_GRPC_STATUS_INTERNAL:             return "internal";
  case FD_GRPC_STATUS_UNAVAILABLE:          return "unavailable";
  case FD_GRPC_STATUS_DATA_LOSS:            return "data loss";
  case FD_GRPC_STATUS_UNAUTHENTICATED:      return "unauthenticated";
  default:                                  return "unknown";
  }
}
