#include <cstring>
#include <thread>
#include <random>
#include <iostream>

namespace rr {
    class RandNum_generator {
    private:
        RandNum_generator(const RandNum_generator&) = delete;

        RandNum_generator& operator=(const RandNum_generator&) = delete;

        std::random_device rd;
        std::default_random_engine e;
        std::uniform_int_distribution<size_t> u;
        int mStart, mEnd;

    public:
        // [start, end], inclusive, uniformally distributed
        RandNum_generator(size_t start, size_t end)
            : u(start, end)
            , e(rd())
            , mStart(start), mEnd(end) {}

        RandNum_generator() { RandNum_generator(0, SIZE_MAX); }

        // [mStart, mEnd], inclusive
        unsigned nextNum() { return u(e); }
        size_t max() { return u.max(); }
        size_t min() { return u.min(); }
    };
}