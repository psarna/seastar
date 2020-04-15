/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright (C) 2019 ScyllaDB Ltd.
 */

#pragma once

#include <cstdint>
#include <ostream>
#include <istream>
#include <array>
#include <vector>
#include <stdexcept>

#include <seastar/net/byteorder.hh>

#include "kafka_error_code.hh"

namespace seastar {

namespace kafka {

struct parsing_exception : public std::runtime_error {
public:
    parsing_exception(const std::string& message) : runtime_error(message) {}
};

template<typename NumberType>
class kafka_number_t {
private:
    NumberType _value;
    static constexpr auto NUMBER_SIZE = sizeof(NumberType);

public:
    kafka_number_t() noexcept : kafka_number_t(0) {}

    explicit kafka_number_t(NumberType value) noexcept : _value(value) {}

    [[nodiscard]] const NumberType& operator*() const noexcept { return _value; }

    [[nodiscard]] NumberType& operator*() noexcept { return _value; }

    kafka_number_t& operator=(NumberType value) noexcept {
        _value = value;
        return *this;
    }

    void serialize(std::ostream& os, int16_t api_version) const {
        std::array<char, NUMBER_SIZE> buffer{};
        auto value = net::hton(_value);
        auto value_pointer = reinterpret_cast<const char*>(&value);
        std::copy(value_pointer, value_pointer + NUMBER_SIZE, buffer.begin());

        os.write(buffer.data(), NUMBER_SIZE);
    }

    void deserialize(std::istream& is, int16_t api_version) {
        std::array<char, NUMBER_SIZE> buffer{};
        is.read(buffer.data(), NUMBER_SIZE);
        if (is.gcount() != NUMBER_SIZE) {
            throw parsing_exception("Stream ended prematurely when reading number");
        }
        _value = net::ntoh(*reinterpret_cast<NumberType*>(buffer.data()));
    }
};

class kafka_error_code_t {
private:
    int16_t _value;
    static constexpr auto NUMBER_SIZE = sizeof(int16_t);

public:
    kafka_error_code_t() noexcept : _value(0) {}
    kafka_error_code_t(const error::kafka_error_code& error) noexcept : _value(error._error_code) {}

    [[nodiscard]] const error::kafka_error_code& operator*() const noexcept {
        return error::kafka_error_code::get_error(_value);
    }

    kafka_error_code_t& operator=(const error::kafka_error_code& error) noexcept {
        _value = error._error_code;
        return *this;
    }

    void serialize(std::ostream& os, int16_t api_version) const {
        std::array<char, NUMBER_SIZE> buffer{};
        auto value = net::hton(_value);
        auto value_pointer = reinterpret_cast<const char*>(&value);
        std::copy(value_pointer, value_pointer + NUMBER_SIZE, buffer.begin());

        os.write(buffer.data(), NUMBER_SIZE);
    }

    void deserialize(std::istream& is, int16_t api_version) {
        std::array<char, NUMBER_SIZE> buffer{};
        is.read(buffer.data(), NUMBER_SIZE);
        if (is.gcount() != NUMBER_SIZE) {
            throw parsing_exception("Stream ended prematurely when reading number");
        }
        _value = net::ntoh(*reinterpret_cast<int16_t*>(buffer.data()));
        try {
            error::kafka_error_code::get_error(_value);
        } catch (const std::out_of_range& e) {
            throw parsing_exception("Error with such code does not exist");
        }
    }

    bool operator==(const error::kafka_error_code& other) const {
        return other._error_code == this->_value;
    }

    bool operator!=(const error::kafka_error_code& other) const {
        return ! (*this == other);
    }
};

using kafka_int8_t = kafka_number_t<int8_t>;
using kafka_int16_t = kafka_number_t<int16_t>;
using kafka_int32_t = kafka_number_t<int32_t>;
using kafka_int64_t = kafka_number_t<int64_t>;
using kafka_uint32_t = kafka_number_t<uint32_t>;
using kafka_bool_t = kafka_number_t<uint8_t>;

class kafka_varint_t {
private:
    int32_t _value;
public:
    kafka_varint_t() noexcept : kafka_varint_t(0) {}

    explicit kafka_varint_t(int32_t value) noexcept : _value(value) {}

    [[nodiscard]] const int32_t& operator*() const noexcept { return _value; }

    [[nodiscard]] int32_t& operator*() noexcept { return _value; }

    kafka_varint_t& operator=(int32_t value) noexcept {
        _value = value;
        return *this;
    }

    void serialize(std::ostream& os, int16_t api_version) const {
        auto current_value = (static_cast<uint32_t>(_value) << 1) ^ static_cast<uint32_t>(_value >> 31);
        do {
            uint8_t current_byte = current_value & 0x7F;
            current_value >>= 7;
            if (current_value != 0) {
                current_byte |= 0x80;
            }
            os.write(reinterpret_cast<const char*>(&current_byte), 1);
        } while (current_value != 0);
    }

    void deserialize(std::istream& is, int16_t api_version) {
        uint32_t current_value = 0;
        int32_t current_offset = 0;
        char current_byte = 0;
        do {
            is.read(&current_byte, 1);
            if (is.gcount() != 1) {
                throw parsing_exception("Stream ended prematurely when reading varint");
            }
            if (current_byte == 0) {
                break;
            }
            auto max_bit_write = current_offset + 32 -  __builtin_clz(static_cast<uint8_t>(current_byte));
            if (max_bit_write > 32) {
                throw parsing_exception("Deserialized varint is larger than 32 bits");
            }
            current_value |= static_cast<int32_t>(current_byte & 0x7F) << current_offset;
            current_offset += 7;
        } while (current_byte & 0x80);
        current_value = (current_value >> 1) ^ -(current_value & 1);
        _value = current_value;
    }
};

template<typename SizeType>
class kafka_buffer_t {
private:
    std::string _value;
public:
    kafka_buffer_t() noexcept = default;

    explicit kafka_buffer_t(std::string value) : _value(std::move(value)) {}

    [[nodiscard]] const std::string& operator*() const noexcept { return _value; }

    [[nodiscard]] std::string& operator*() noexcept { return _value; }

    [[nodiscard]] const std::string* operator->() const noexcept { return &_value; }

    [[nodiscard]] std::string* operator->() noexcept { return &_value; }

    kafka_buffer_t& operator=(const std::string& value) {
        _value = value;
        return *this;
    }

    kafka_buffer_t& operator=(std::string&& value) noexcept {
        _value = std::move(value);
        return *this;
    }

    void serialize(std::ostream& os, int16_t api_version) const {
        SizeType length(_value.size());
        length.serialize(os, api_version);

        os.write(_value.data(), _value.size());
    }

    void deserialize(std::istream& is, int16_t api_version) {
        SizeType length;
        length.deserialize(is, api_version);
        // TODO: Max length check
        if (*length < 0) {
            throw parsing_exception("Length of buffer is negative");
        }

        std::string value;
        value.resize(*length);
        is.read(value.data(), *length);

        if (is.gcount() != *length) {
            throw parsing_exception("Stream ended prematurely when reading buffer");
        }
        _value.swap(value);
    }
};

template<typename SizeType>
class kafka_nullable_buffer_t {
private:
    std::string _value;
    bool _is_null;
public:
    kafka_nullable_buffer_t() noexcept : _is_null(true) {}

    explicit kafka_nullable_buffer_t(std::string value) : _value(std::move(value)), _is_null(false) {}

    [[nodiscard]] bool is_null() const noexcept { return _is_null; }

    void set_null() noexcept {
        _value.clear();
        _is_null = true;
    }

    [[nodiscard]] const std::string& operator*() const {
        if (_is_null) {
            throw std::domain_error("Object is null.");
        }
        return _value;
    }

    [[nodiscard]] std::string& operator*() {
        if (_is_null) {
            throw std::domain_error("Object is null.");
        }
        return _value;
    }

    [[nodiscard]] const std::string* operator->() const {
        if (_is_null) {
            throw std::domain_error("Object is null.");
        }
        return &_value;
    }

    [[nodiscard]] std::string* operator->() {
        if (_is_null) {
            throw std::domain_error("Object is null.");
        }
        return &_value;
    }

    kafka_nullable_buffer_t& operator=(const std::string& value) {
        _value = value;
        _is_null = false;
        return *this;
    }

    kafka_nullable_buffer_t& operator=(std::string&& value) noexcept {
        _value = std::move(value);
        _is_null = false;
        return *this;
    }

    void serialize(std::ostream& os, int16_t api_version) const {
        if (_is_null) {
            SizeType null_indicator(-1);
            null_indicator.serialize(os, api_version);
        } else {
            SizeType length(_value.size());
            length.serialize(os, api_version);
            os.write(_value.data(), _value.size());
        }
    }

    void deserialize(std::istream& is, int16_t api_version) {
        SizeType length;
        length.deserialize(is, api_version);
        if (*length >= 0) {
            std::string value;
            // TODO: Max length check
            value.resize(*length);
            is.read(value.data(), *length);

            if (is.gcount() != *length) {
                throw parsing_exception("Stream ended prematurely when reading nullable buffer");
            }
            _value.swap(value);
            _is_null = false;
        } else if (*length == -1) {
            set_null();
        } else {
            throw parsing_exception("Length of buffer is invalid");
        }
    }
};

using kafka_string_t = kafka_buffer_t<kafka_int16_t>;
using kafka_nullable_string_t = kafka_nullable_buffer_t<kafka_int16_t>;

using kafka_bytes_t = kafka_buffer_t<kafka_int32_t>;
using kafka_nullable_bytes_t = kafka_nullable_buffer_t<kafka_int32_t>;

template<typename ElementType, typename ElementCountType = kafka_int32_t>
class kafka_array_t {
private:
    std::vector<ElementType> _elems;
    bool _is_null;
public:
    kafka_array_t() noexcept : _is_null(true) {}

    explicit kafka_array_t(std::vector<ElementType> elems) noexcept
            : _elems(std::move(elems)), _is_null(false) {}

    [[nodiscard]] bool is_null() const noexcept { return _is_null; }

    [[nodiscard]] ElementType& operator[](size_t i) {
        if (_is_null) {
            throw std::domain_error("Object is null.");
        }
        return _elems[i];
    }

    [[nodiscard]] const ElementType& operator[](size_t i) const {
        if (_is_null) {
            throw std::domain_error("Object is null.");
        }
        return _elems[i];
    }

    [[nodiscard]] const std::vector<ElementType>& operator*() const {
        if (_is_null) {
            throw std::domain_error("Object is null.");
        }
        return _elems;
    }

    [[nodiscard]] std::vector<ElementType>& operator*() {
        if (_is_null) {
            throw std::domain_error("Object is null.");
        }
        return _elems;
    }

    [[nodiscard]] const std::vector<ElementType>* operator->() const {
        if (_is_null) {
            throw std::domain_error("Object is null.");
        }
        return &_elems;
    }

    [[nodiscard]] std::vector<ElementType>* operator->() {
        if (_is_null) {
            throw std::domain_error("Object is null.");
        }
        return &_elems;
    }

    kafka_array_t& operator=(const std::vector<ElementType>& elems) {
        _elems = elems;
        _is_null = false;
        return *this;
    }

    kafka_array_t& operator=(std::vector<ElementType>&& elems) noexcept {
        _elems = std::move(elems);
        _is_null = false;
        return *this;
    }

    void set_null() noexcept {
        _elems.clear();
        _is_null = true;
    }

    void serialize(std::ostream& os, int16_t api_version) const {
        if (_is_null) {
            ElementCountType null_indicator(-1);
            null_indicator.serialize(os, api_version);
        } else {
            ElementCountType length(_elems.size());
            length.serialize(os, api_version);
            for (const auto& elem : _elems) {
                elem.serialize(os, api_version);
            }
        }
    }

    void deserialize(std::istream& is, int16_t api_version) {
        ElementCountType length;
        length.deserialize(is, api_version);
        if (*length >= 0) {
            // TODO: Max length check
            std::vector<ElementType> elems(*length);
            for (int32_t i = 0; i < *length; i++) {
                elems[i].deserialize(is, api_version);
            }
            _elems.swap(elems);
            _is_null = false;
        } else if (*length == -1) {
            set_null();
        } else {
            throw parsing_exception("Length of array is invalid");
        }
    }
};

}

}
