/* FireBAM part of fd_keyguard_match.c, included once before
   fd_keyguard_payload_match.

   BAM auth: the 24 byte FD_BAM_AUTH_LABEL (fd_bam_tile.h), with its
   NUL, then 1 to 128 (sizeof(bam_api_AuthChallengeResponse)) challenge
   bytes */

FD_FN_PURE static int
fd_keyguard_payload_matches_bam_auth( uchar const * data,
                                      ulong         sz,
                                      int           sign_type ) {
  if( sign_type!=FD_KEYGUARD_SIGN_TYPE_ED25519 ) return 0;
  if( sz<=24UL || sz>24UL+128UL                ) return 0;
  return !memcmp( data, "X_OFF_CHAIN_JITO_BAM_V1", 24UL );
}
