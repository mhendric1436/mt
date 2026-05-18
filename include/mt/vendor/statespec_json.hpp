#pragma once

// Vendored from ~/repos/statespec/bindings/cpp/json.hpp
// Update by copying that file here when the upstream changes.

#include <cctype>
#include <charconv>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <initializer_list>
#include <iomanip>
#include <limits>
#include <locale>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace statespec::backend
{

class JsonError : public std::runtime_error
{
  public:
    explicit JsonError(std::string message)
        : std::runtime_error(std::move(message))
    {
    }
};

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
            throw JsonError("JSON number must be finite");
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

    static Json null()
    {
        return Json(nullptr);
    }

    static Json array(Array values)
    {
        return Json(std::move(values));
    }

    static Json array(std::initializer_list<Json> values)
    {
        return Json(Array(values));
    }

    static Json object(Object values)
    {
        return Json(std::move(values));
    }

    static Json object(std::initializer_list<Member> values)
    {
        Object object;
        for (const auto& [key, value] : values)
        {
            auto [_, inserted] = object.emplace(key, value);
            if (!inserted)
            {
                throw JsonError("duplicate JSON object member: " + key);
            }
        }
        return Json(std::move(object));
    }

    static Json parse(std::string_view encoded);

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

    bool is_number() const noexcept
    {
        return is_int64() || is_double();
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
        if (is_int64())
        {
            return static_cast<double>(as_int64());
        }
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

    bool contains(std::string_view key) const
    {
        if (!is_object())
        {
            return false;
        }
        const auto& object = as_object();
        return object.find(std::string(key)) != object.end();
    }

    const Json* find(std::string_view key) const
    {
        if (!is_object())
        {
            return nullptr;
        }
        const auto& object = as_object();
        auto iter = object.find(std::string(key));
        if (iter == object.end())
        {
            return nullptr;
        }
        return &iter->second;
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
        stream.imbue(std::locale::classic());
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

    Value value_ = nullptr;
};

class JsonParser
{
  public:
    explicit JsonParser(std::string_view input)
        : input_(input)
    {
    }

    Json parse()
    {
        auto value = parse_value();
        skip_whitespace();
        if (position_ != input_.size())
        {
            fail("unexpected trailing JSON content");
        }
        return value;
    }

  private:
    [[noreturn]] void fail(std::string_view message) const
    {
        throw JsonError(std::string(message));
    }

    bool consume(char expected)
    {
        skip_whitespace();
        if (position_ < input_.size() && input_[position_] == expected)
        {
            ++position_;
            return true;
        }
        return false;
    }

    void expect(char expected)
    {
        if (!consume(expected))
        {
            fail("unexpected JSON token");
        }
    }

    bool starts_with(std::string_view value) const
    {
        return input_.substr(position_, value.size()) == value;
    }

    void skip_whitespace()
    {
        while (position_ < input_.size())
        {
            auto c = static_cast<unsigned char>(input_[position_]);
            if (c != ' ' && c != '\n' && c != '\r' && c != '\t')
            {
                return;
            }
            ++position_;
        }
    }

    Json parse_value()
    {
        skip_whitespace();
        if (position_ >= input_.size())
        {
            fail("missing JSON value");
        }
        if (starts_with("null"))
        {
            position_ += 4;
            return Json::null();
        }
        if (starts_with("true"))
        {
            position_ += 4;
            return Json(true);
        }
        if (starts_with("false"))
        {
            position_ += 5;
            return Json(false);
        }

        switch (input_[position_])
        {
        case '"':
            return Json(parse_string());
        case '[':
            return parse_array();
        case '{':
            return parse_object();
        default:
            return parse_number();
        }
    }

    std::string parse_string()
    {
        expect('"');
        std::string out;
        while (position_ < input_.size())
        {
            auto c = static_cast<unsigned char>(input_[position_++]);
            if (c == '"')
            {
                return out;
            }
            if (c < 0x20)
            {
                fail("unescaped control character in JSON string");
            }
            if (c != '\\')
            {
                out.push_back(static_cast<char>(c));
                continue;
            }
            if (position_ >= input_.size())
            {
                fail("unterminated JSON escape");
            }

            auto escaped = input_[position_++];
            switch (escaped)
            {
            case '"':
            case '\\':
            case '/':
                out.push_back(escaped);
                break;
            case 'b':
                out.push_back('\b');
                break;
            case 'f':
                out.push_back('\f');
                break;
            case 'n':
                out.push_back('\n');
                break;
            case 'r':
                out.push_back('\r');
                break;
            case 't':
                out.push_back('\t');
                break;
            case 'u':
                append_unicode_escape(out);
                break;
            default:
                fail("invalid JSON escape");
            }
        }
        fail("unterminated JSON string");
    }

    unsigned parse_hex_quad()
    {
        if (position_ + 4 > input_.size())
        {
            fail("short JSON unicode escape");
        }

        auto value = 0U;
        for (auto i = 0; i < 4; ++i)
        {
            value = (value << 4) | hex_value(input_[position_++]);
        }
        return value;
    }

    void append_unicode_escape(std::string& out)
    {
        auto code_point = parse_hex_quad();
        if (code_point >= 0xD800 && code_point <= 0xDBFF)
        {
            if (position_ + 6 > input_.size() || input_[position_] != '\\' ||
                input_[position_ + 1] != 'u')
            {
                fail("missing JSON low surrogate");
            }
            position_ += 2;
            auto low = parse_hex_quad();
            if (low < 0xDC00 || low > 0xDFFF)
            {
                fail("invalid JSON low surrogate");
            }
            code_point = 0x10000 + (((code_point - 0xD800) << 10) | (low - 0xDC00));
        }
        else if (code_point >= 0xDC00 && code_point <= 0xDFFF)
        {
            fail("unexpected JSON low surrogate");
        }
        append_utf8(out, code_point);
    }

    unsigned hex_value(char c) const
    {
        if (c >= '0' && c <= '9')
        {
            return static_cast<unsigned>(c - '0');
        }
        if (c >= 'a' && c <= 'f')
        {
            return static_cast<unsigned>(c - 'a' + 10);
        }
        if (c >= 'A' && c <= 'F')
        {
            return static_cast<unsigned>(c - 'A' + 10);
        }
        fail("invalid JSON unicode escape");
    }

    static void append_utf8(
        std::string& out,
        unsigned code_point
    )
    {
        if (code_point <= 0x7F)
        {
            out.push_back(static_cast<char>(code_point));
        }
        else if (code_point <= 0x7FF)
        {
            out.push_back(static_cast<char>(0xC0 | (code_point >> 6)));
            out.push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
        }
        else if (code_point <= 0xFFFF)
        {
            out.push_back(static_cast<char>(0xE0 | (code_point >> 12)));
            out.push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
        }
        else if (code_point <= 0x10FFFF)
        {
            out.push_back(static_cast<char>(0xF0 | (code_point >> 18)));
            out.push_back(static_cast<char>(0x80 | ((code_point >> 12) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
        }
        else
        {
            throw JsonError("invalid JSON unicode code point");
        }
    }

    Json parse_array()
    {
        expect('[');
        Json::Array values;
        if (consume(']'))
        {
            return Json::array(std::move(values));
        }

        while (true)
        {
            values.push_back(parse_value());
            if (consume(']'))
            {
                return Json::array(std::move(values));
            }
            expect(',');
        }
    }

    Json parse_object()
    {
        expect('{');
        Json::Object object;
        if (consume('}'))
        {
            return Json::object(std::move(object));
        }

        while (true)
        {
            skip_whitespace();
            if (position_ >= input_.size() || input_[position_] != '"')
            {
                fail("JSON object member name must be a string");
            }
            auto key = parse_string();
            expect(':');
            auto [_, inserted] = object.emplace(std::move(key), parse_value());
            if (!inserted)
            {
                fail("duplicate JSON object member");
            }
            if (consume('}'))
            {
                return Json::object(std::move(object));
            }
            expect(',');
        }
    }

    Json parse_number()
    {
        auto start = position_;
        if (position_ < input_.size() && input_[position_] == '-')
        {
            ++position_;
        }
        if (position_ >= input_.size())
        {
            fail("invalid JSON number");
        }

        if (input_[position_] == '0')
        {
            ++position_;
            if (position_ < input_.size() &&
                std::isdigit(static_cast<unsigned char>(input_[position_])))
            {
                fail("invalid JSON number leading zero");
            }
        }
        else if (input_[position_] >= '1' && input_[position_] <= '9')
        {
            while (position_ < input_.size() &&
                   std::isdigit(static_cast<unsigned char>(input_[position_])))
            {
                ++position_;
            }
        }
        else
        {
            fail("invalid JSON number");
        }

        auto is_double = false;
        if (position_ < input_.size() && input_[position_] == '.')
        {
            is_double = true;
            ++position_;
            if (position_ >= input_.size() ||
                !std::isdigit(static_cast<unsigned char>(input_[position_])))
            {
                fail("invalid JSON number fraction");
            }
            while (position_ < input_.size() &&
                   std::isdigit(static_cast<unsigned char>(input_[position_])))
            {
                ++position_;
            }
        }

        if (position_ < input_.size() && (input_[position_] == 'e' || input_[position_] == 'E'))
        {
            is_double = true;
            ++position_;
            if (position_ < input_.size() && (input_[position_] == '+' || input_[position_] == '-'))
            {
                ++position_;
            }
            if (position_ >= input_.size() ||
                !std::isdigit(static_cast<unsigned char>(input_[position_])))
            {
                fail("invalid JSON number exponent");
            }
            while (position_ < input_.size() &&
                   std::isdigit(static_cast<unsigned char>(input_[position_])))
            {
                ++position_;
            }
        }

        auto text = std::string(input_.substr(start, position_ - start));
        if (is_double)
        {
            char* end = nullptr;
            auto value = std::strtod(text.c_str(), &end);
            if (end != text.c_str() + text.size() || !std::isfinite(value))
            {
                fail("invalid JSON number");
            }
            return Json(value);
        }

        std::int64_t value = 0;
        auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
        if (ec != std::errc{} || ptr != text.data() + text.size())
        {
            fail("invalid JSON integer");
        }
        return Json(value);
    }

    std::string_view input_;
    std::size_t position_ = 0;
};

inline Json Json::parse(std::string_view encoded)
{
    return JsonParser(encoded).parse();
}

inline Json parse_json(std::string_view encoded)
{
    return Json::parse(encoded);
}

} // namespace statespec::backend
