skeleton: skeleton.o
        g++ -L/usr/local/berkeleydb/lib -o $@ $< -ldb_cxx

skeleton.o : skeleton.cpp
        g++ -I/usr/local/berkeleydb/include -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c -o $@ $<
