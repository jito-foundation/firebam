CC:=hfuzz-clang
CXX:=hfuzz-clang++
LD:=hfuzz-clang++
CPPFLAGS+=-fno-omit-frame-pointer

FD_HAS_FUZZ:=1

ifeq ($(FD_USING_CLANG),1)
CPPFLAGS+=-Wno-unknown-attributes
endif
