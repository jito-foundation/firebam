#ifndef HEADER_fd_bam_errors
#define HEADER_fd_bam_errors

/* Central definitions for user-visible BAM error strings.
   Keep format strings and prefixes in one place so tests can
   assert on them without duplicating literals. */

#define FD_BAM_ERR_MSG_BUILDER_INFO_UNAVAILABLE "builder info unavailable"
#define FD_BAM_ERR_MSG_BUNDLE_EXECUTION_FAILED  "bundle execution failed"

#define FD_BAM_ERR_FMT_TRANSACTION_ERROR        "transaction error %u"
#define FD_BAM_ERR_PREFIX_TRANSACTION_ERROR     "transaction error "

#define FD_BAM_ERR_FMT_INVALID_SCHEDULING_ERROR "invalid scheduling error %u"
#define FD_BAM_ERR_PREFIX_INVALID_SCHEDULING    "invalid scheduling error "

#endif /* HEADER_fd_bam_errors */
