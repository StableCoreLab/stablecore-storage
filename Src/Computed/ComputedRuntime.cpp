#include "StableCore/Storage/Computed.h"

#include <algorithm>
#include <cmath>
#include <cwctype>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

#include "StableCore/Storage/RefCounted.h"

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

ErrorCode ReadScalarFromContext(IComputedContext* context, const std::wstring& fieldName, Scalar* outScalar)
{
    if (context == nullptr || outScalar == nullptr)
    {
        return SC_E_POINTER;
    }

    Value value;
    const ErrorCode rc = context->GetValue(fieldName.c_str(), &value);
    if (Failed(rc))
    {
        return rc;
    }

    switch (value.GetKind())
    {
    case ValueKind::Int64:
    {
        std::int64_t number = 0;
        value.AsInt64(&number);
        *outScalar = Scalar{ValueKind::Int64, static_cast<double>(number), number != 0};
        return SC_OK;
    }
    case ValueKind::Double:
    {
        double number = 0.0;
        value.AsDouble(&number);
        *outScalar = Scalar{ValueKind::Double, number, number != 0.0};
        return SC_OK;
    }
    case ValueKind::Bool:
    {
        bool flag = false;
        value.AsBool(&flag);
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

Scalar MakeNumeric(double value, ValueKind kindHint = ValueKind::Double) noexcept
{
    return Scalar{kindHint, value, value != 0.0};
}

class ExpressionParser
{
public:
    ExpressionParser(const ComputedColumnDef& column, IComputedContext* context)
        : column_(column)
        , context_(context)
        , lexer_(column.expression)
        , current_(lexer_.Next())
    {
    }

    ErrorCode Evaluate(Value* outValue)
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
                Scalar value;
                const ErrorCode rc = ParseExpression(&value);
                if (Failed(rc))
                {
                    return rc;
                }
                args.push_back(value);

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

    ErrorCode ConvertToValue(const Scalar& scalar, Value* outValue) const
    {
        switch (column_.valueKind)
        {
        case ValueKind::Int64:
            *outValue = Value::FromInt64(static_cast<std::int64_t>(std::llround(scalar.numberValue)));
            return SC_OK;
        case ValueKind::Double:
            *outValue = Value::FromDouble(scalar.numberValue);
            return SC_OK;
        case ValueKind::Bool:
            *outValue = Value::FromBool(IsTruthy(scalar));
            return SC_OK;
        default:
            return SC_E_TYPE_MISMATCH;
        }
    }

    void Advance()
    {
        current_ = lexer_.Next();
    }

    const ComputedColumnDef& column_;
    IComputedContext* context_{nullptr};
    Lexer lexer_;
    Token current_;
};

class ExpressionEvaluator final : public IComputedEvaluator, public RefCountedObject
{
public:
    ErrorCode Evaluate(const ComputedColumnDef& column, IComputedContext* context, Value* outValue) override
    {
        ExpressionParser parser(column, context);
        return parser.Evaluate(outValue);
    }
};

class DefaultRuleRegistry final : public IRuleRegistry, public RefCountedObject
{
public:
    ErrorCode Register(const wchar_t* ruleId, IComputedEvaluator* evaluator) override
    {
        if (ruleId == nullptr || *ruleId == L'\0')
        {
            return SC_E_INVALIDARG;
        }
        if (evaluator == nullptr)
        {
            return SC_E_POINTER;
        }

        evaluators_[ruleId] = ComputedEvaluatorPtr(evaluator);
        return SC_OK;
    }

    ErrorCode Find(const wchar_t* ruleId, ComputedEvaluatorPtr& outEvaluator) override
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
    std::unordered_map<std::wstring, ComputedEvaluatorPtr> evaluators_;
};

struct CacheKeyHash
{
    std::size_t operator()(const ComputedCacheKey& key) const noexcept
    {
        return static_cast<std::size_t>(key.recordId)
            ^ (std::hash<std::wstring>{}(key.columnName) << 1)
            ^ (static_cast<std::size_t>(key.version) << 2);
    }
};

bool DependencyMatchesChange(const FieldDependency& dependency, const DataChange& change) noexcept
{
    return (dependency.tableName.empty() || dependency.tableName == change.tableName)
        && dependency.fieldName == change.fieldName;
}

class ComputedCache final : public IComputedCache, public RefCountedObject
{
public:
    ErrorCode TryGet(const ComputedCacheKey& key, Value* outValue) override
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

        *outValue = it->second.value;
        return SC_OK;
    }

    ErrorCode Put(const ComputedCacheEntry& entry) override
    {
        entries_[entry.key] = entry;
        return SC_OK;
    }

    ErrorCode Invalidate(const ChangeSet& changeSet, const std::vector<ComputedColumnDef>& computedColumns) override
    {
        std::vector<ComputedCacheKey> removeKeys;
        for (const auto& [key, entry] : entries_)
        {
            for (const auto& column : computedColumns)
            {
                if (column.name != key.columnName)
                {
                    continue;
                }
                if (DoesDependencySetIntersect(entry.dependencies, changeSet))
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
    std::unordered_map<ComputedCacheKey, ComputedCacheEntry, CacheKeyHash> entries_;
};

}  // namespace

ErrorCode CreateDefaultExpressionEvaluator(ComputedEvaluatorPtr& outEvaluator)
{
    outEvaluator = MakeRef<ExpressionEvaluator>();
    return SC_OK;
}

ErrorCode CreateDefaultRuleRegistry(RuleRegistryPtr& outRegistry)
{
    outRegistry = MakeRef<DefaultRuleRegistry>();
    return SC_OK;
}

ErrorCode CreateComputedCache(ComputedCachePtr& outCache)
{
    outCache = MakeRef<ComputedCache>();
    return SC_OK;
}

ErrorCode EvaluateComputedColumn(
    const ComputedColumnDef& column,
    IComputedContext* context,
    IRuleRegistry* ruleRegistry,
    Value* outValue)
{
    if (context == nullptr || outValue == nullptr)
    {
        return SC_E_POINTER;
    }

    switch (column.kind)
    {
    case ComputedFieldKind::Expression:
    {
        ComputedEvaluatorPtr evaluator;
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
        ComputedEvaluatorPtr evaluator;
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

bool DoesDependencySetIntersect(const ComputedDependencySet& dependencies, const ChangeSet& changeSet) noexcept
{
    for (const auto& change : changeSet.changes)
    {
        const auto factIt = std::find_if(
            dependencies.factFields.begin(),
            dependencies.factFields.end(),
            [&](const FieldDependency& dependency)
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
            [&](const FieldDependency& dependency)
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
