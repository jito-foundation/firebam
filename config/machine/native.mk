define _map-define
  ifeq ($(shell echo | $(CC) -march=native -E -dM - | grep -c $(2)),1)
    CPPFLAGS+=-D$(1)=1
    $(1):=1
  endif
endef

map-define = $(eval $(call _map-define,$(1),$(2)))

define _check-define
    $(eval $(1) := $(shell echo | $(CC) -march=native -E -dM - | grep -q $(2) && echo 1))
endef

check-define = $(eval $(call _check-define,$(1),$(2)))

CC?=gcc

$(call check-define, FD_USING_CLANG, __clang__)

$(call check-define, FD_IS_GNU, __GNUC__)
ifeq ($(FD_IS_GNU),1)
    ifneq ($(FD_USING_CLANG),1)
        FD_USING_GCC := 1
    endif
endif

ifdef FD_USING_GCC
include config/base.mk
	CC:=gcc
	CXX:=g++
	LD:=g++
  FD_COMPILER_MAJOR_VERSION:=$(shell echo | $(CC) -march=native -E -dM - | grep __GNUC__ | awk '{print $$3}')
include config/extra/with-gcc.mk
else ifdef FD_USING_CLANG
include config/base.mk
	CC=clang
	CXX=clang++
	LD=clang++
  FD_COMPILER_MAJOR_VERSION:=$(shell echo | $(CC) -march=native -E -dM - | grep __clang_major__ |  awk '{print $$3}')
include config/extra/with-clang.mk
endif

BUILDDIR?=native/$(CC)
# Build for a baseline x86-64 target while explicitly disabling AVX/AVX2/AVX512
# so the native profile stays portable across CPUs without those extensions.
CPPFLAGS+=-march=x86-64 -mtune=native -msse4.2 -mcx16 -mno-avx -mno-avx2 -mno-avx512f -mno-fma
RUSTFLAGS+=-C target-cpu=x86-64 -C target-feature=+sse4.2,-avx,-avx2,-avx512f,-fma

include config/extra/with-brutality.mk
include config/extra/with-optimization.mk
include config/extra/with-debug.mk
include config/extra/with-security.mk

$(call map-define,FD_HAS_SHANI, __SHA__)
$(call map-define,FD_HAS_INT128, __SIZEOF_INT128__)
FD_HAS_DOUBLE:=1
CPPFLAGS+=-DFD_HAS_DOUBLE=1
$(call map-define,FD_HAS_ALLOCA, __linux__)
$(call map-define,FD_HAS_THREADS, __linux__)
$(call map-define,FD_HAS_X86, __x86_64__)
$(call map-define,FD_HAS_SSE, __SSE4_2__)
$(call map-define,FD_IS_X86_64, __x86_64__)
$(call map-define,FD_HAS_AESNI, __AES__)

ifdef FD_HAS_AESNI
CPPFLAGS+=-maes -mpclmul
RUSTFLAGS+=-C target-feature=+aes,+pclmul
endif

ifdef FD_HAS_SHANI
CPPFLAGS+=-msha
RUSTFLAGS+=-C target-feature=+sha
endif

# Keep AVX/AVX512 empty so AVX-gated sources stay out of the build.
FD_HAS_AVX:=
FD_HAS_AVX512:=
FD_HAS_AVX512_MESSAGE:=(Disabled in native.mk)

ifdef FD_HAS_THREADS
include config/extra/with-threads.mk
endif

ifdef FD_IS_X86_64
include config/extra/with-x86-64.mk
$(info Using FD_HAS_SSE=$(FD_HAS_SSE))
$(info Using FD_HAS_AVX=$(FD_HAS_AVX))
$(info Using FD_HAS_AVX512=$(FD_HAS_AVX512) $(FD_HAS_AVX512_MESSAGE))
$(info Using FD_HAS_SHANI=$(FD_HAS_SHANI))
$(info Using FD_HAS_AESNI=$(FD_HAS_AESNI))
endif
