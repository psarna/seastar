#include "path.hh"
#include <algorithm>

namespace seastar::fs::path {

bool has_suffix(const std::string &str, const std::string &suffix) noexcept {
    return str.size() >= suffix.size() && str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
}

std::string path_filename(const std::string &path) noexcept {
    auto pos = path.rfind('/');
    return path.substr(pos == std::string::npos ? 0 : pos + 1);
}

bool is_canonical(const std::string &path) noexcept {
    if (path.empty()) {
        return false;
    }

    if (path.front() == '.' || path.front() != '/') {
        return false;
    }

    if (has_suffix(path, "/.")) {
        return false;
    }

    char prev = path.front();
    return std::none_of(std::next(path.begin()), path.end(), [&prev](char cur) {
        const auto is_duplicated = (cur == '/' || cur == '.') && cur == prev;
        prev = cur;
        return is_duplicated;
    });
}

std::string canonical(std::string path, std::string curr_dir) {
    if (path.empty()) {
        return curr_dir;
    }

    std::string curr_path = std::move(curr_dir);
    if (path.front() == '/') {
        curr_path = '/';
    }

    if (!has_suffix(curr_path, "/")) {
        curr_path += '/';
    }

    auto erase_last_component = [&curr_path] {
        // Remove trailing '/' to help trim last component
        if (has_suffix(curr_path, "/") && curr_path != "/") {
            curr_path.pop_back();
        }

        curr_path.resize(curr_path.size() - path_filename(curr_path).size());
    };

    auto process_component = [&curr_path, &erase_last_component](std::string component) {
        if (component == "..") {
            erase_last_component();
            return;
        }

        // Current path is a directory
        if (!has_suffix(curr_path, "/") && !curr_path.empty()) {
            curr_path += '/';
        }

        if (component == ".") {
            return;
        }

        curr_path.append(component);
    };

    for (size_t i = 0; i < path.size(); ++i) {
        if (path[i] == '/') {
            continue;
        }

        size_t next_slash_pos = std::min(path.find('/', i + 1), path.size());
        process_component(path.substr(i, next_slash_pos - i));
        i = next_slash_pos;
    }

    if (has_suffix(path, "/") && !has_suffix(curr_path, "/")) {
        curr_path += '/'; // Add trimmed trailing slash
    }

    return curr_path;
}

std::string root_entry(const std::string &path) {
    auto first_slash_pos = path.find('/', 0);
    auto beg = first_slash_pos == std::string::npos ? 0 : first_slash_pos + 1;

    auto second_slash_pos = path.find('/', beg);
    auto end = second_slash_pos == std::string::npos ? path.size() : second_slash_pos - 1;

    return path.substr(beg, end);
}

bool is_root_entry(const std::string& path) noexcept {
    if (path.empty()) {
        return false;
    }
    auto nb_slashes = std::count(path.begin(), path.end(), '/');
    nb_slashes -= path.back() == '/'; // removing trailing slash
    return nb_slashes == 1;
}

}
