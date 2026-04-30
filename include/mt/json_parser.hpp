#pragma once

#include "mt/errors.hpp"
#include "mt/hash.hpp"
#include "mt/json.hpp"

#include <cctype>
#include <cstdint>
#include <exception>
#include <string>
#include <string_view>
#include <utility>

// -----------------------------------------------------------------------------
// mt/json_parser.hpp
//
// Small JSON parser for backend storage and tests that need to decode canonical
// mt::Json text without depending on backend-specific implementation units.
// -----------------------------------------------------------------------------

namespace mt
{

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
        if (position_ != input_.size())
        {
            fail();
        }
        return value;
    }

  private:
    [[noreturn]] void fail() const
    {
        throw BackendError("invalid JSON value");
    }

    bool consume(char expected)
    {
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
            fail();
        }
    }

    bool starts_with(std::string_view value) const
    {
        return input_.substr(position_, value.size()) == value;
    }

    Json parse_value()
    {
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
        if (position_ >= input_.size())
        {
            fail();
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
            auto c = input_[position_++];
            if (c == '"')
            {
                return out;
            }
            if (c != '\\')
            {
                out.push_back(c);
                continue;
            }
            if (position_ >= input_.size())
            {
                fail();
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
                out.push_back(parse_basic_unicode_escape());
                break;
            default:
                fail();
            }
        }
        fail();
    }

    char parse_basic_unicode_escape()
    {
        if (position_ + 4 > input_.size())
        {
            fail();
        }

        auto value = 0U;
        for (auto i = 0; i < 4; ++i)
        {
            value = (value << 4) | hex_value(input_[position_++]);
        }
        if (value > 0x7F)
        {
            fail();
        }
        return static_cast<char>(value);
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
            auto key = parse_string();
            expect(':');
            object.insert_or_assign(std::move(key), parse_value());
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
        consume('-');
        if (position_ >= input_.size() ||
            !std::isdigit(static_cast<unsigned char>(input_[position_])))
        {
            fail();
        }

        while (position_ < input_.size() &&
               std::isdigit(static_cast<unsigned char>(input_[position_])))
        {
            ++position_;
        }

        auto is_double = false;
        if (consume('.'))
        {
            is_double = true;
            if (position_ >= input_.size() ||
                !std::isdigit(static_cast<unsigned char>(input_[position_])))
            {
                fail();
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
                fail();
            }
            while (position_ < input_.size() &&
                   std::isdigit(static_cast<unsigned char>(input_[position_])))
            {
                ++position_;
            }
        }

        auto text = std::string(input_.substr(start, position_ - start));
        try
        {
            if (is_double)
            {
                return Json(std::stod(text));
            }
            return Json(static_cast<std::int64_t>(std::stoll(text)));
        }
        catch (const std::exception&)
        {
            fail();
        }
    }

  private:
    std::string_view input_;
    std::size_t position_ = 0;
};

inline Json parse_json(std::string_view encoded)
{
    return JsonParser(encoded).parse();
}

} // namespace mt
