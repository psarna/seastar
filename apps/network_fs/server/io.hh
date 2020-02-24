#pragma once

#include <seastar/core/do_with.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <utility>
#include <vector>

namespace internal {

template<typename... Cont>
seastar::future<> then_seq(Cont&&... conts) {
    auto fut = seastar::make_ready_future<>();
    ((fut = std::move(fut).then(std::forward<Cont>(conts))), ...);
    return fut;
}

}

template<typename T>
inline
seastar::future<> read_object(seastar::input_stream<char>& input, T& obj) {
    return input.read_exactly(sizeof(T)).then([&obj] (seastar::temporary_buffer<char> buff) {
        if (buff.size() != sizeof(T)) {
            return seastar::make_exception_future<>(std::runtime_error("Couldn't read object"));
        }
        std::memcpy(&obj, buff.get(), sizeof(T));
        return seastar::make_ready_future<>(); // TODO: add htons etc?
    });
}

// sstring format - length:size_t string:char[]
template <>
inline
seastar::future<> read_object(seastar::input_stream<char>& input, seastar::sstring& str) {
    return seastar::do_with((size_t)0, [&input, &str] (size_t& size) {
        return read_object(input, size).then([&input, &size] () {
            return input.read_exactly(size);
        }).then([&str] (seastar::temporary_buffer<char> buff) {
            str = seastar::sstring(buff.begin(), buff.end());
        });
    });
}

// temporary_buffer format - length:size_t string:char[]
template <>
inline
seastar::future<> read_object(seastar::input_stream<char>& input, seastar::temporary_buffer<char>& buff) {
    return seastar::do_with((size_t)0, [&input, &buff] (size_t& size) {
        return read_object(input, size).then([&input, &size] () {
            return input.read_exactly(size);
        }).then([&buff] (seastar::temporary_buffer<char> tmp_buff) {
            buff = std::move(tmp_buff);
        });
    });
}

template<typename... T>
inline
seastar::future<> read_objects(seastar::input_stream<char>& input, T&... objects) {
    return internal::then_seq([&input, &objects] {
        return read_object(input, objects);
    }...);
}

template<typename T>
inline
seastar::future<> write_object(seastar::output_stream<char>& output, const T& obj) {
    return output.write(reinterpret_cast<const char*>(&obj), sizeof(obj));
}

// sstring format - length:size_t string:char[]
template<>
inline
seastar::future<> write_object(seastar::output_stream<char>& output, const seastar::sstring& str) {
    return write_object(output, str.size()).then([&output, &str] {
        return output.write(str);
    });
}

// temporary_buffer format - length:size_t string:char[]
template<>
inline
seastar::future<> write_object(seastar::output_stream<char>& output, const seastar::temporary_buffer<char>& buff) {
    return write_object(output, buff.size()).then([&output, &buff] {
        return output.write(buff.get(), buff.size());
    });
}

// sstring vector format - length:size_t elem1:sstring elem2:sstring ...
template<>
inline
seastar::future<> write_object(seastar::output_stream<char>& output, const std::vector<seastar::sstring>& vec) {
    return write_object(output, vec.size()).then([&output, &vec] {
        return seastar::do_for_each(vec, [&output] (const seastar::sstring& obj) {
            return write_object<seastar::sstring>(output, obj);
        });
    });
}

template<typename... T>
inline
seastar::future<> write_objects(seastar::output_stream<char>& output, T&&... objects) {
    return seastar::do_with(std::forward<T>(objects)..., [&output] (auto&... args) {
        return internal::then_seq([&output, &args] {
            return write_object(output, std::move(args));
        }...);
    });
}
