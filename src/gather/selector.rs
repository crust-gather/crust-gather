use kube::Resource;
use logos::{Lexer, Logos, Span};
use serde::Deserialize;

type Error = (String, Span);

type Result<T> = std::result::Result<T, Error>;

#[derive(Deserialize, Clone, Debug)]
pub struct Selector {
    #[serde(rename = "labelSelector")]
    label_selector: Option<String>,
}

impl Selector {
    pub fn matches<R: Resource>(&self, res: R) -> Option<R> {
        match &self.label_selector {
            Some(selector) => Expressions::try_from(selector.clone())
                .ok()?
                .into_iter()
                .map(|selector| Selector::matches_selector(&selector, &res))
                .all(|matches| matches)
                .then_some(res),
            None => Some(res),
        }
    }

    fn matches_selector<R: Resource>(selector: &Expression, res: &R) -> bool {
        let labels = res.meta().labels.as_ref();
        let not = |found: Option<()>| match found {
            Some(_) => None,
            None => Some(()),
        };
        let exist = |key: &String| labels?.get(key).map(|_| ());
        let value_in =
            |key: &String, values: &Vec<String>| values.contains(labels?.get(key)?).then_some(());
        let matches = |key: &String, value: &String| {
            let found = labels?.get(key)? == value;
            found.then_some(())
        };

        match selector {
            Expression::Set(set) => match set {
                Set::Exist(key) => exist(key),
                Set::NotExist(key) => not(exist(key)),
                Set::In(key, values) => value_in(key, values),
                Set::NotIn(key, values) => not(value_in(key, values)),
            },
            Expression::Equality(equality) => match equality {
                Equality::Equal(key, value) => matches(key, value),
                Equality::NotEqual(key, value) => not(matches(key, value)),
            },
        }
        .is_some()
    }
}

pub struct Expressions(Vec<Expression>);

impl IntoIterator for Expressions {
    type Item = Expression;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Logos, Debug, PartialEq)]
#[logos(skip r"[, \t\n\f]+")]
pub enum Expression {
    #[regex(r"\w+\s+in\s+\([\w\s,]+\)", |lex| parse_set(lex.slice()))]
    #[regex(r"\w+\s+notin\s+\([\w\s,]+\)", |lex| parse_set(lex.slice()))]
    #[regex(r"\!\w+", |lex| parse_set(lex.slice()))]
    #[regex(r"\w+", |lex| parse_set(lex.slice()))]
    Set(Set),

    #[regex(r"\w+\s*=\s*\w+", |lex| parse_equality(lex.slice()))]
    #[regex(r"\w+\s*==\s*\w+", |lex| parse_equality(lex.slice()))]
    #[regex(r"\w+\s*!=\s*\w+", |lex| parse_equality(lex.slice()))]
    Equality(Equality),
}

impl TryFrom<String> for Expressions {
    type Error = Error;

    fn try_from(selector: String) -> Result<Self> {
        let mut lexer = Expression::lexer(selector.as_str());
        let mut expressions = vec![];
        while let Some(value) = parse_expression(&mut lexer)? {
            expressions.push(value);
        }

        Ok(Expressions(expressions))
    }
}

#[derive(Debug, PartialEq)]
pub enum Equality {
    Equal(String, String),
    NotEqual(String, String),
}

#[derive(Logos, Debug, PartialEq)]
#[logos(skip r"[ \t\n\f]+")]
enum EqualityToken {
    #[token("=")]
    #[token("==")]
    Equal,
    #[token("!=")]
    NotEqual,
    #[regex(r"\w+", |lex| lex.slice().to_owned())]
    Value(String),
}

#[derive(Debug, PartialEq)]
pub enum Set {
    Exist(String),
    NotExist(String),
    In(String, Vec<String>),
    NotIn(String, Vec<String>),
}

#[derive(Logos, Debug, PartialEq)]
#[logos(skip r"[, \t\n\f]+")]
enum SetToken {
    #[token("!")]
    Not,

    #[regex(r"\([\w\s,]+\)", |lex| parse_value_list(lex.slice()))]
    ValuesList(Vec<String>),

    #[token("in")]
    In,

    #[token("notin")]
    NotIn,

    #[regex(r"\w+", |lex| lex.slice().to_owned())]
    Value(String),
}

#[derive(Logos, Debug, PartialEq)]
#[logos(skip r"[, \(\)\t\n\f]+")]
enum ValuesListToken {
    #[regex(r"[a-zA-Z_-]+", |lex| lex.slice().to_owned())]
    Value(String),
}

/// Parse selector expression
pub fn parse_expression(lexer: &mut Lexer<'_, Expression>) -> Result<Option<Expression>> {
    lexer
        .next()
        .map(|token| match token {
            Ok(Expression::Equality(eq)) => Ok(Expression::Equality(eq)),
            Ok(Expression::Set(set)) => Ok(Expression::Set(set)),
            _ => Err(("unexpected expression".to_owned(), lexer.span())),
        })
        .transpose()
}

/// Parse an equality based expression.
fn parse_equality(source: &str) -> Option<Equality> {
    let mut lexer = EqualityToken::lexer(source);
    let key = lexer.next()?.ok()?;
    let op = lexer.next()?.ok()?;
    let value = lexer.next()?.ok()?;
    match (key, op, value) {
        (EqualityToken::Value(key), EqualityToken::Equal, EqualityToken::Value(value)) => {
            Some(Equality::Equal(key, value))
        }
        (EqualityToken::Value(key), EqualityToken::NotEqual, EqualityToken::Value(value)) => {
            Some(Equality::NotEqual(key, value))
        }
        _ => None,
    }
}

/// Parse a set based expression.
fn parse_set(source: &str) -> Option<Set> {
    let mut lexer = SetToken::lexer(source);
    let key = lexer.next()?.ok()?;
    match key {
        SetToken::Not => match lexer.next()?.ok()? {
            SetToken::Value(value) => Some(Set::NotExist(value)),
            _ => None,
        },
        SetToken::Value(key) => {
            let op = match lexer.next() {
                Some(op) => op.ok()?,
                None => return Some(Set::Exist(key)),
            };
            let value = lexer.next()?.ok()?;
            match (op, value) {
                (SetToken::In, SetToken::ValuesList(values)) => Some(Set::In(key, values)),
                (SetToken::NotIn, SetToken::ValuesList(values)) => Some(Set::NotIn(key, values)),
                (_, _) => None,
            }
        }
        SetToken::ValuesList(_) | SetToken::In | SetToken::NotIn => None,
    }
}

// Parse a list of values into vector
fn parse_value_list(source: &str) -> Option<Vec<String>> {
    let lexer = ValuesListToken::lexer(source);
    let mut values = vec![];
    for value in lexer {
        values.push(match value.ok()? {
            ValuesListToken::Value(value) => value,
        });
    }

    Some(values)
}

#[cfg(test)]
mod tests {
    use logos::Logos;

    use super::{parse_expression, parse_value_list, Equality, Expression, Set};

    #[test]
    fn values_lexer() {
        assert_eq!(
            Some(vec!["a".into(), "b".into(), "c".into()]),
            parse_value_list(" (a,b, c)")
        );
        assert_eq!(Some(vec!["a".into()]), parse_value_list("(a)"));
        assert_eq!(Some(vec![]), parse_value_list("()"));
        assert_eq!(Some(vec![]), parse_value_list(""));
    }

    #[test]
    fn expression_lexer() {
        let data = "a==b,,b=c,c!=d,a in (a,b, c), a notin (a), c,!a,a()d";
        let mut lexer = Expression::lexer(data);
        assert_eq!(
            Some(Expression::Equality(Equality::Equal(
                "a".into(),
                "b".into()
            ))),
            parse_expression(&mut lexer).unwrap()
        );
        assert_eq!(
            Some(Expression::Equality(Equality::Equal(
                "b".into(),
                "c".into()
            ))),
            parse_expression(&mut lexer).unwrap()
        );
        assert_eq!(
            Some(Expression::Equality(Equality::NotEqual(
                "c".into(),
                "d".into()
            ))),
            parse_expression(&mut lexer).unwrap()
        );
        assert_eq!(
            Some(Expression::Set(Set::In(
                "a".into(),
                vec!["a".into(), "b".into(), "c".into()]
            ))),
            parse_expression(&mut lexer).unwrap()
        );
        assert_eq!(
            Some(Expression::Set(Set::NotIn("a".into(), vec!["a".into()]))),
            parse_expression(&mut lexer).unwrap()
        );
        assert_eq!(
            Some(Expression::Set(Set::Exist("c".into()))),
            parse_expression(&mut lexer).unwrap()
        );
        assert_eq!(
            Some(Expression::Set(Set::NotExist("a".into()))),
            parse_expression(&mut lexer).unwrap()
        );
        assert_eq!(
            Some(Expression::Set(Set::Exist("a".into()))),
            parse_expression(&mut lexer).unwrap()
        );
        assert_eq!(
            Some(("unexpected expression".into(), 49..50)),
            parse_expression(&mut lexer).err()
        );
        assert_eq!(
            Some(("unexpected expression".into(), 50..51)),
            parse_expression(&mut lexer).err()
        );
        assert_eq!(
            Some(Expression::Set(Set::Exist("d".into()))),
            parse_expression(&mut lexer).unwrap()
        );
        assert_eq!(None, parse_expression(&mut lexer).unwrap());
    }
}
