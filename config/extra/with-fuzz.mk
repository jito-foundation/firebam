include config/extra/with-handholding.mk

FD_HAS_FUZZ:=1

# Clang ignores GCC's nonstring attribute and warns with -Wunknown-attributes,
# which becomes fatal under -Werror.  Suppress for fuzz builds only.
ifeq ($(FD_USING_CLANG),1)
CPPFLAGS+=-Wno-unknown-attributes
endif

CPPFLAGS+=-fno-omit-frame-pointer
CPPFLAGS+=-fsanitize=fuzzer-no-link
CPPFLAGS+=-fsanitize-coverage=inline-8bit-counters,pc-table

LDFLAGS+=-fsanitize-coverage=inline-8bit-counters,pc-table
LDFLAGS_FUZZ+=-fsanitize=fuzzer
