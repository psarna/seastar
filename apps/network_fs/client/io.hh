#pragma once

#include <cstring>
#include <errno.h>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

template<typename T>
struct fixed_buf_string {
    T* buff;
    size_t size;
    fixed_buf_string(T* buff)
        : buff(buff), size(std::strlen(buff)) {}
    fixed_buf_string(T* buff, size_t size)
        : buff(buff), size(size) {}
};

size_t read_exactly(int fd, size_t num, char* buff) noexcept {
    size_t pos = 0;
    while (num > 0) {
        int rd = read(fd, buff + pos, num);
        if (rd <= 0) {
            return pos;
        }
        num -= rd;
        pos += rd;
    }
    return pos;
}

template<typename T>
int read_object(int fd, T& obj) {
    T tmp;
    if (read_exactly(fd, sizeof(tmp), reinterpret_cast<char*>(&tmp)) != sizeof(tmp)) {
        return -EIO;
    }

    obj = std::move(tmp);
    return 0;
}

template <>
int read_object(int fd, std::string& str) {
    int ret = 0;

    size_t size;
    if ((ret = read_object(fd, size)) != 0) {
        return ret;
    }

    std::string tmp(size, 0);
    if (read_exactly(fd, size, tmp.data()) != size) {
        return -EIO;
    }

    str = std::move(tmp);
    return 0;
}

template <>
int read_object(int fd, fixed_buf_string<char>& str) {
    int ret = 0;

    size_t size;
    if ((ret = read_object(fd, size)) != 0) {
        return ret;
    }

    size_t acc_read;
    if ((acc_read = read_exactly(fd, size, str.buff)) != size) {
        return -EIO;
    }
    str.size = size;

    return 0;
}

template <>
int read_object(int fd, std::vector<std::string>& vec) {
    int ret = 0;

    size_t size;
    if ((ret = read_object(fd, size)) != 0) {
        return ret;
    }

    std::vector<std::string> tmp_vec;
    while (size--) {
        std::string tmp_str(size, 0);
        if ((ret = read_object(fd, tmp_str)) != 0) {
            return ret;
        }
        tmp_vec.emplace_back(std::move(tmp_str));
    }

    vec = std::move(tmp_vec);
    return 0;
}

template<typename... T>
int read_objects(int fd, T&... objects) {
    int ret = 0;
    (void) (((ret = read_object(fd, objects)) == 0) && ...);
    return ret;
}

size_t write_exactly(int fd, const char* buff, size_t num) noexcept {
    size_t pos = 0;
    while (num > 0) {
        int rd = write(fd, buff + pos, num);
        if (rd <= 0) {
            return pos;
        }
        num -= rd;
        pos += rd;
    }
    return pos;
}

template<typename T>
int write_object(int fd, const T& obj) noexcept {
    char buff[sizeof(obj)];
    std::memcpy(buff, &obj, sizeof(obj));
    return write_exactly(fd, buff, sizeof(obj)) == sizeof(obj) ? 0 : -EIO;
}

template<>
int write_object(int fd, const std::string& str) noexcept {
    int ret = write_object(fd, str.size());
    if (ret) {
        return ret;
    }
    return write_exactly(fd, str.data(), str.size()) == str.size() ? 0 : -EIO;
}

template<>
int write_object(int fd, const fixed_buf_string<const char>& str) noexcept {
    int ret = write_object(fd, str.size);
    if (ret) {
        return ret;
    }
    return write_exactly(fd, str.buff, str.size) == str.size ? 0 : -EIO;
}

template<typename... T>
int write_objects(int fd, T&&... objects) noexcept {
    int ret = 0;
    (void) (((ret = write_object(fd, objects)) == 0) && ...);
    return ret;
}
