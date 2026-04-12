#include "StableCore/Storage/ISCComputed.h"

#include <algorithm>
#include <cmath>
#include <cwctype>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

#include "StableCore/Storage/SCRefCounted.h"

namespace stablecore::storage
{
namespace
{

struct Token
{
    enum class Kind
    {
        End,
        Number,
        Identifier,
        Plus,
        Minus,
        Star,
        Slash,
        Comma,
        LParen,
        RParen,
    };

    Kind kind{Kind::End};
    std::wstring text;
};

class Lexer
{
public:
    explicit Lexer(std::wstring source)
        : source_(std::move(source))
    {
    }

    Token Next()
    {
        SkipWhitespace();
        if (index_ >= source_.size())
        {
            return {};
        }

        const wchar_t ch = source_[index_];
        if (std::iswdigit(ch) != 0 || ch == L'.')
        {
            return ReadNumber();
        }
        if (std::iswalpha(ch) != 0 || ch == L'_')
        {
            return ReadIdentifier();
        }

        ++index_;
        switch (ch)
        {
        case L'+': return Token{Token::Kind::Plus, L"+"};
        case L'-': return Token{Token::Kind::Minus, L"-"};
        case L'*': return Token{Token::Kind::Star, L"*"};
        case L'/': return Token{Token::Kind::Slash, L"/"};
        case L',': return Token{Token::Kind::Comma, L","};
        case L'(': return Token{Token::Kind::LParen, L"("};
        case L')': return Token{Token::Kind::RParen, L")"};
        default: return {};
        }
    }

private:
    void SkipWhitespace()
    {
        while (index_ < source_.size() && std::iswspace(source_[index_]) != 0)
        {
            ++index_;
        }
    }

    Token ReadNumber()
    {
        const std::size_t start = index_;
        bool sawDot = false;
        while (index_ < source_.size())
        {
            const wchar_t ch = source_[index_];
            if (std::iswdigit(ch) != 0)
            {
                ++index_;
                continue;
            }
            if (ch == L'.' && !sawDot)
            {
                sawDot = true;
                ++index_;
                continue;
            }
            break;
        }
        return Token{Token::Kind::Number, source_.substr(start, index_ - start)};
    }

    Token ReadIdentifier()
    {
        const std::size_t start = index_;
        while (index_ < source_.size())
        {
            const wchar_t ch = source_[index_];
            if (std::iswalnum(ch) != 0 || ch == L'_' || ch == L'.')
            {
                ++index_;
                continue;
            }
            break;
        }
        return Token{Token::Kind::Identifier, source_.substr(start, index_ - start)};
    }

    std::wstring source_;
    std::size_t index_{0};
};

struct Scalar
{
    ValueKind kind{ValueKind::Null};
    double numberValue{0.0};
    bool boolValue{false};
};

ErrorCode ReadScalarFromContext(ISCComputedContext* context, const std::wstring& fieldName, Scalar* outScalar)
{
    if (context == nullptr || outScalar == nullptr)
    {
        return SC_E_POINTER;
    }

    SCValue SCValue;
    const ErrorCode rc = context->GetValue(fieldName.c_str(), &SCValue);
    if (Failed(rc))
    {
        return rc;
    }

    switch (SCValue.GetKind())
    {
    case ValueKind::Int64:
    {
        std::int64_t number = 0;
        SCValue.AsInt64(&number);
        *outScalar = Scalar{ValueKind::Int64, static_cast<double>(number), number != 0};
        return SC_OK;
    }
    case ValueKind::Double:
    {
        double number = 0.0;
        SCValue.AsDouble(&number);
        *outScalar = Scalar{ValueKind::Double, number, number != 0.0};
        return SC_OK;
    }
    case ValueKind::Bool:
    {
        bool flag = false;
        SCValue.AsBool(&flag);
        *outScalar = Scalar{ValueKind::Bool, flag ? 1.0 : 0.0, flag};
        return SC_OK;
    }
    default:
        return SC_E_TYPE_MISMATCH;
    }
}

bool IsTruthy(const Scalar& scalar) noexcept
{
    if (scalar.kind == ValueKind::Bool)
    {
        return scalar.boolValue;
    }
    return scalar.numberValue != 0.0;
}

Scalar MakeNumeric(double SCValue, ValueKind kindHint = ValueKind::Double) noexcept
{
    return Scalar{kindHint, SCValue, SCValue != 0.0};
}

class ExpressionParser
{
public:
    ExpressionParser(const SCComputedColumnDef& column, ISCComputedContext* context)
        : column_(column)
        , context_(context)
        , lexer_(column.expression)
        , current_(lexer_.Next())
    {
    }

    ErrorCode Evaluate(SCValue* outValue)
    {
        if (outValue == nullptr)
        {
            return SC_E_POINTER;
        }
        if (column_.expression.empty())
        {
            return SC_E_INVALIDARG;
        }

        Scalar scalar;
        const ErrorCode rc = ParseExpression(&scalar);
        if (Failed(rc))
        {
            return rc;
        }
        if (current_.kind != Token::Kind::End)
        {
            return SC_E_INVALIDARG;
        }
        return ConvertToValue(scalar, outValue);
    }

private:
    ErrorCode ParseExpression(Scalar* outScalar)
    {
        Scalar left;
        ErrorCode rc = ParseTerm(&left);
        if (Failed(rc))
        {
            return rc;
        }

        while (current_.kind == Token::Kind::Plus || current_.kind == Token::Kind::Minus)
        {
            const Token::Kind op = current_.kind;
            Advance();

            Scalar right;
            rc = ParseTerm(&right);
            if (Failed(rc))
            {
                return rc;
            }

            left = MakeNumeric(
                op == Token::Kind::Plus ? left.numberValue + right.numberValue : left.numberValue - right.numberValue);
        }

        *outScalar = left;
        return SC_OK;
    }

    ErrorCode ParseTerm(Scalar* outScalar)
    {
        Scalar left;
        ErrorCode rc = ParseFactor(&left);
        if (Failed(rc))
        {
            return rc;
        }

        while (current_.kind == Token::Kind::Star || current_.kind == Token::Kind::Slash)
        {
            const Token::Kind op = current_.kind;
            Advance();

            Scalar right;
            rc = ParseFactor(&right);
            if (Failed(rc))
            {
                return rc;
            }
            if (op == Token::Kind::Slash && right.numberValue == 0.0)
            {
                return SC_E_INVALIDARG;
            }

            left = MakeNumeric(
                op == Token::Kind::Star ? left.numberValue * right.numberValue : left.numberValue / right.numberValue);
        }

        *outScalar = left;
        return SC_OK;
    }

    ErrorCode ParseFactor(Scalar* outScalar)
    {
        if (current_.kind == Token::Kind::Minus)
        {
            Advance();
            Scalar inner;
            const ErrorCode rc = ParseFactor(&inner);
            if (Failed(rc))
            {
                return rc;
            }
            *outScalar = MakeNumeric(-inner.numberValue);
            return SC_OK;
        }

        if (current_.kind == Token::Kind::Number)
        {
            const std::wstring text = current_.text;
            Advance();
            *outScalar = MakeNumeric(std::stod(text), text.find(L'.') == std::wstring::npos ? ValueKind::Int64 : ValueKind::Double);
            return SC_OK;
        }

        if (current_.kind == Token::Kind::Identifier)
        {
            const std::wstring identifier = current_.text;
            Advance();
            if (current_.kind == Token::Kind::LParen)
            {
                return ParseFunctionCall(identifier, outScalar);
            }
            return ReadScalarFromContext(context_, identifier, outScalar);
        }

        if (current_.kind == Token::Kind::LParen)
        {
            Advance();
            const ErrorCode rc = ParseExpression(outScalar);
            if (Failed(rc))
            {
                return rc;
            }
            if (current_.kind != Token::Kind::RParen)
            {
                return SC_E_INVALIDARG;
            }
            Advance();
            return SC_OK;
        }

        return SC_E_INVALIDARG;
    }

    ErrorCode ParseFunctionCall(const std::wstring& name, Scalar* outScalar)
    {
        Advance();  // (
        std::vector<Scalar> args;
        if (current_.kind != Token::Kind::RParen)
        {
            while (true)
            {
                Scalar SCValue;
                const ErrorCode rc = ParseExpression(&SCValue);
                if (Failed(rc))
                {
                    return rc;
                }
                args.push_back(SCValue);

                if (current_.kind == Token::Kind::Comma)
                {
                    Advance();
                    continue;
                }
                break;
            }
        }

        if (current_.kind != Token::Kind::RParen)
        {
            return SC_E_INVALIDARG;
        }
        Advance();

        if (name == L"min" && args.size() == 2)
        {
            *outScalar = MakeNumeric(std::min(args[0].numberValue, args[1].numberValue));
            return SC_OK;
        }
        if (name == L"max" && args.size() == 2)
        {
            *outScalar = MakeNumeric(std::max(args[0].numberValue, args[1].numberValue));
            return SC_OK;
        }
        if (name == L"abs" && args.size() == 1)
        {
            *outScalar = MakeNumeric(std::fabs(args[0].numberValue));
            return SC_OK;
        }
        if (name == L"if" && args.size() == 3)
        {
            *outScalar = IsTruthy(args[0]) ? args[1] : args[2];
            return SC_OK;
        }

        return SC_E_INVALIDARG;
    }

    ErrorCode ConvertToValue(const Scalar& scalar, SCValue* outValue) const
    {
        switch (column_.valueKind)
        {
        case ValueKind::Int64:
            *outValue = SCValue::FromInt64(static_cast<std::int64_t>(std::llround(scalar.numberValue)));
            return SC_OK;
        case ValueKind::Double:
            *outValue = SCValue::FromDouble(scalar.numberValue);
            return SC_OK;
        case ValueKind::Bool:
            *outValue = SCValue::FromBool(IsTruthy(scalar));
            return SC_OK;
        default:
            return SC_E_TYPE_MISMATCH;
        }
    }

    void Advance()
    {
        current_ = lexer_.Next();
    }

    const SCComputedColumnDef& column_;
    ISCComputedContext* context_{nullptr};
    Lexer lexer_;
    Token current_;
};

class ExpressionEvaluator final : public ISCComputedEvaluator, public SCRefCountedObject
{
public:
    ErrorCode Evaluate(const SCComputedColumnDef& column, ISCComputedContext* context, SCValue* outValue) override
    {
        ExpressionParser parser(column, context);
        return parser.Evaluate(outValue);
    }
};

class DefaultRuleRegistry final : public ISCRuleRegistry, public SCRefCountedObject
{
public:
    ErrorCode Register(const wchar_t* ruleId, ISCComputedEvaluator* evaluator) override
    {
        if (ruleId == nullptr || *ruleId == L'\0')
        {
            return SC_E_INVALIDARG;
        }
        if (evaluator == nullptr)
        {
            return SC_E_POINTER;
        }

        evaluators_[ruleId] = SCComputedEvaluatorPtr(evaluator);
        return SC_OK;
    }

    ErrorCode Find(const wchar_t* ruleId, SCComputedEvaluatorPtr& outEvaluator) override
    {
        if (ruleId == nullptr || *ruleId == L'\0')
        {
            return SC_E_INVALIDARG;
        }

        const auto it = evaluators_.find(ruleId);
        if (it == evaluators_.end())
        {
            return SC_E_RECORD_NOT_FOUND;
        }

        outEvaluator = it->second;
        return SC_OK;
    }

private:
    std::unordered_map<std::wstring, SCComputedEvaluatorPtr> evaluators_;
};

struct CacheKeyHash
{
    std::size_t operator()(const SCComputedCacheKey& key) const noexcept
    {
        return static_cast<std::size_t>(key.recordId)
            ^ (std::hash<std::wstring>{}(key.columnName) << 1)
            ^ (static_cast<std::size_t>(key.version) << 2);
    }
};

bool DependencyMatchesChange(const SCFieldDependency& dependency, const SCDataChange& change) noexcept
{
    return (dependency.tableName.empty() || dependency.tableName == change.tableName)
        && dependency.fieldName == change.fieldName;
}

class ComputedCache final : public ISCComputedCache, public SCRefCountedObject
{
public:
    ErrorCode TryGet(const SCComputedCacheKey& key, SCValue* outValue) override
    {
        if (outValue == nullptr)
        {
            return SC_E_POINTER;
        }

        const auto it = entries_.find(key);
        if (it == entries_.end())
        {
            return SC_FALSE_RESULT;
        }

        *outValue = it->second.SCValue;
        return SC_OK;
    }

    ErrorCode Put(const SCComputedCacheEntry& entry) override
    {
        entries_[entry.key] = entry;
        return SC_OK;
    }

    ErrorCode Invalidate(const SCChangeSet& SCChangeSet, const std::vector<SCComputedColumnDef>& computedColumns) override
    {
        std::vector<SCComputedCacheKey> removeKeys;
        for (const auto& [key, entry] : entries_)
        {
            for (const auto& column : computedColumns)
            {
                if (column.name != key.columnName)
                {
                    continue;
                }
                if (DoesDependencySetIntersect(entry.dependencies, SCChangeSet))
                {
                    removeKeys.push_back(key);
                    break;
                }
            }
        }

        for (const auto& key : removeKeys)
        {
            entries_.erase(key);
        }
        return SC_OK;
    }

    ErrorCode Clear() override
    {
        entries_.clear();
        return SC_OK;
    }

private:
    std::unordered_map<SCComputedCacheKey, SCComputedCacheEntry, CacheKeyHash> entries_;
};

}  // namespace

ErrorCode CreateDefaultExpressionEvaluator(SCComputedEvaluatorPtr& outEvaluator)
{
    outEvaluator = SCMakeRef<ExpressionEvaluator>();
    return SC_OK;
}

ErrorCode CreateDefaultRuleRegistry(SCRuleRegistryPtr& outRegistry)
{
    outRegistry = SCMakeRef<DefaultRuleRegistry>();
    return SC_OK;
}

ErrorCode CreateComputedCache(SCComputedCachePtr& outCache)
{
    outCache = SCMakeRef<ComputedCache>();
    return SC_OK;
}

ErrorCode EvaluateComputedColumn(
    const SCComputedColumnDef& column,
    ISCComputedContext* context,
    ISCRuleRegistry* ruleRegistry,
    SCValue* outValue)
{
    if (context == nullptr || outValue == nullptr)
    {
        return SC_E_POINTER;
    }

    switch (column.kind)
    {
    case ComputedFieldKind::Expression:
    {
        SCComputedEvaluatorPtr evaluator;
        CreateDefaultExpressionEvaluator(evaluator);
        return evaluator->Evaluate(column, context, outValue);
    }
    case ComputedFieldKind::Rule:
    case ComputedFieldKind::Aggregate:
    {
        if (ruleRegistry == nullptr)
        {
            return SC_E_POINTER;
        }
        SCComputedEvaluatorPtr evaluator;
        const ErrorCode findRc = ruleRegistry->Find(column.ruleId.c_str(), evaluator);
        if (Failed(findRc))
        {
            return findRc;
        }
        return evaluator->Evaluate(column, context, outValue);
    }
    default:
        return SC_E_NOTIMPL;
    }
}

bool DoesDependencySetIntersect(const SCComputedDependencySet& dependencies, const SCChangeSet& SCChangeSet) noexcept
{
    for (const auto& change : SCChangeSet.changes)
    {
        const auto factIt = std::find_if(
            dependencies.factFields.begin(),
            dependencies.factFields.end(),
            [&](const SCFieldDependency& dependency)
            {
                return DependencyMatchesChange(dependency, change);
            });
        if (factIt != dependencies.factFields.end())
        {
            return true;
        }

        if (!change.relationChange)
        {
            continue;
        }

        const auto relationIt = std::find_if(
            dependencies.relationFields.begin(),
            dependencies.relationFields.end(),
            [&](const SCFieldDependency& dependency)
            {
                return DependencyMatchesChange(dependency, change);
            });
        if (relationIt != dependencies.relationFields.end())
        {
            return true;
        }
    }

    return false;
}

}  // namespace stablecore::storage
