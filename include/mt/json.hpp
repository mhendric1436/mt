#pragma once

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <iomanip>
#include <limits>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

// -----------------------------------------------------------------------------
// mt/json.hpp
//
// Basic JSON value and stable hashing.
// -----------------------------------------------------------------------------

namespace mt
{

class Json
{
  public:
    using Array = std::vector<Json>;
    using Member = std::pair<std::string, Json>;
    using Object = std::map<std::string, Json>;

    Json() = default;

    Json(std::nullptr_t) {}

    Json(bool value)
        : value_(value)
    {
    }

    Json(int value)
        : value_(static_cast<std::int64_t>(value))
    {
    }

    Json(std::int64_t value)
        : value_(value)
    {
    }

    Json(double value)
        : value_(value)
    {
        if (!std::isfinite(value))
        {
            throw std::invalid_argument("Json double must be finite");
        }
    }

    Json(const char* value)
        : value_(std::string(value))
    {
    }

    Json(std::string value)
        : value_(std::move(value))
    {
    }

    Json(Array value)
        : value_(std::move(value))
    {
    }

    Json(Object value)
        : value_(std::move(value))
    {
    }

    static Json array(std::initializer_list<Json> values)
    {
        return Json(Array(values));
    }

    static Json object(std::initializer_list<Member> values)
    {
        Object object;
        for (auto&& [key, value] : values)
        {
            object.insert_or_assign(key, value);
        }
        return Json(std::move(object));
    }

    bool is_null() const noexcept
    {
        return std::holds_alternative<std::nullptr_t>(value_);
    }

    bool is_bool() const noexcept
    {
        return std::holds_alternative<bool>(value_);
    }

    bool is_int64() const noexcept
    {
        return std::holds_alternative<std::int64_t>(value_);
    }

    bool is_double() const noexcept
    {
        return std::holds_alternative<double>(value_);
    }

    bool is_string() const noexcept
    {
        return std::holds_alternative<std::string>(value_);
    }

    bool is_array() const noexcept
    {
        return std::holds_alternative<Array>(value_);
    }

    bool is_object() const noexcept
    {
        return std::holds_alternative<Object>(value_);
    }

    bool as_bool() const
    {
        return std::get<bool>(value_);
    }

    std::int64_t as_int64() const
    {
        return std::get<std::int64_t>(value_);
    }

    double as_double() const
    {
        return std::get<double>(value_);
    }

    const std::string& as_string() const
    {
        return std::get<std::string>(value_);
    }

    const Array& as_array() const
    {
        return std::get<Array>(value_);
    }

    const Object& as_object() const
    {
        return std::get<Object>(value_);
    }

    const Json& at(const std::string& key) const
    {
        return as_object().at(key);
    }

    const Json& operator[](const std::string& key) const
    {
        return at(key);
    }

    std::string canonical_string() const
    {
        std::string out;
        append_canonical(out);
        return out;
    }

    friend bool operator==(
        const Json&,
        const Json&
    ) = default;

  private:
    using Value =
        std::variant<std::nullptr_t, bool, std::int64_t, double, std::string, Array, Object>;

    void append_canonical(std::string& out) const
    {
        std::visit([&](const auto& value) { append_value(out, value); }, value_);
    }

    static void append_value(
        std::string& out,
        std::nullptr_t
    )
    {
        out += "null";
    }

    static void append_value(
        std::string& out,
        bool value
    )
    {
        out += value ? "true" : "false";
    }

    static void append_value(
        std::string& out,
        std::int64_t value
    )
    {
        out += std::to_string(value);
    }

    static void append_value(
        std::string& out,
        double value
    )
    {
        std::ostringstream stream;
        stream << std::setprecision(std::numeric_limits<double>::max_digits10) << value;
        auto encoded = stream.str();
        if (encoded.find_first_of(".eE") == std::string::npos)
        {
            encoded += ".0";
        }
        out += encoded;
    }

    static void append_value(
        std::string& out,
        const std::string& value
    )
    {
        append_escaped_string(out, value);
    }

    static void append_value(
        std::string& out,
        const Array& value
    )
    {
        out.push_back('[');
        bool first = true;
        for (const auto& item : value)
        {
            if (!first)
            {
                out.push_back(',');
            }
            first = false;
            item.append_canonical(out);
        }
        out.push_back(']');
    }

    static void append_value(
        std::string& out,
        const Object& value
    )
    {
        out.push_back('{');
        bool first = true;
        for (const auto& [key, item] : value)
        {
            if (!first)
            {
                out.push_back(',');
            }
            first = false;
            append_escaped_string(out, key);
            out.push_back(':');
            item.append_canonical(out);
        }
        out.push_back('}');
    }

    static void append_escaped_string(
        std::string& out,
        const std::string& value
    )
    {
        out.push_back('"');
        for (unsigned char c : value)
        {
            switch (c)
            {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\b':
                out += "\\b";
                break;
            case '\f':
                out += "\\f";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                if (c < 0x20)
                {
                    constexpr char hex[] = "0123456789abcdef";
                    out += "\\u00";
                    out.push_back(hex[(c >> 4) & 0x0f]);
                    out.push_back(hex[c & 0x0f]);
                }
                else
                {
                    out.push_back(static_cast<char>(c));
                }
                break;
            }
        }
        out.push_back('"');
    }

  private:
    Value value_ = nullptr;
};

struct Hash
{
    std::vector<std::uint8_t> bytes;

    friend bool operator==(
        const Hash&,
        const Hash&
    ) = default;
};

inline Hash hash_json(const Json& value)
{
    constexpr std::uint64_t fnv_offset = 14695981039346656037ULL;
    constexpr std::uint64_t fnv_prime = 1099511628211ULL;

    auto canonical = value.canonical_string();
    auto hash = fnv_offset;

    for (unsigned char byte : canonical)
    {
        hash ^= byte;
        hash *= fnv_prime;
    }

    std::vector<std::uint8_t> bytes(sizeof(hash));
    for (std::size_t i = 0; i < bytes.size(); ++i)
    {
        bytes[i] = static_cast<std::uint8_t>((hash >> ((bytes.size() - 1 - i) * 8)) & 0xff);
    }

    return Hash{std::move(bytes)};
}

} // namespace mt
