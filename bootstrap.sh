#! /bin/sh -e

autoreconf -i
CXXFLAGS='-O3' ./configure --disable-assertions --enable-max-key-len=65536 --with-malloc=malloc
#echo "Now, run ./configure."
