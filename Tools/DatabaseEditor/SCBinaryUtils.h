#pragma once

#include <cstdint>
#include <vector>

#include <QString>

namespace StableCore::Storage::Editor
{

    inline int HexDigitValue(QChar ch)
    {
        if (ch >= QLatin1Char('0') && ch <= QLatin1Char('9'))
        {
            return ch.unicode() - '0';
        }
        if (ch >= QLatin1Char('a') && ch <= QLatin1Char('f'))
        {
            return 10 + (ch.unicode() - 'a');
        }
        if (ch >= QLatin1Char('A') && ch <= QLatin1Char('F'))
        {
            return 10 + (ch.unicode() - 'A');
        }
        return -1;
    }

    inline QString BinaryToHex(const std::vector<std::uint8_t>& value)
    {
        QString text = QStringLiteral("0x");
        text.reserve(2 + static_cast<int>(value.size()) * 2);
        static constexpr char kHexDigits[] = "0123456789ABCDEF";
        for (std::uint8_t byte : value)
        {
            text.push_back(QLatin1Char(kHexDigits[(byte >> 4) & 0x0F]));
            text.push_back(QLatin1Char(kHexDigits[byte & 0x0F]));
        }
        return text;
    }

    inline bool ParseBinaryHex(const QString& text,
                               std::vector<std::uint8_t>* outValue,
                               QString* outError)
    {
        if (outValue == nullptr)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral("Output value is null.");
            }
            return false;
        }

        QString cleaned = text.trimmed();
        if (cleaned.startsWith(QStringLiteral("0x"), Qt::CaseInsensitive))
        {
            cleaned.remove(0, 2);
        }

        QString hex;
        hex.reserve(cleaned.size());
        for (const QChar ch : cleaned)
        {
            if (ch.isSpace() || ch == QLatin1Char('_'))
            {
                continue;
            }
            hex.push_back(ch);
        }

        if ((hex.size() % 2) != 0)
        {
            if (outError != nullptr)
            {
                *outError = QStringLiteral(
                    "Binary values must contain an even number of hex digits.");
            }
            return false;
        }

        outValue->clear();
        outValue->reserve(static_cast<std::size_t>(hex.size() / 2));
        for (qsizetype index = 0; index < hex.size(); index += 2)
        {
            const int high = HexDigitValue(hex[index]);
            const int low = HexDigitValue(hex[index + 1]);
            if (high < 0 || low < 0)
            {
                if (outError != nullptr)
                {
                    *outError = QStringLiteral(
                        "Binary values must use hex digits only.");
                }
                return false;
            }
            outValue->push_back(static_cast<std::uint8_t>((high << 4) | low));
        }
        return true;
    }

}  // namespace StableCore::Storage::Editor
