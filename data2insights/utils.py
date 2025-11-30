import click
from collections import defaultdict


TYPE_KEYWORDS = {
    "gcloud": ["foo", "bar", "baz"],
    "local": ["alpha", "beta"],
    "screen": ["abc", "def"],
    "duckdb": ["connection-string"],
}

SOURCE_KEYWORDS = TYPE_KEYWORDS.copy()


def make_keyword_validator(type_to_keywords: dict):
    """Return a Click callback that validates keywords based on a type parameter."""

    def validator(ctx, param, value):
        type_value = ctx.params.get("type")
        if type_value and value:
            allowed = type_to_keywords.get(type_value, [])
            invalid = [v for v in value if v not in allowed]
            if invalid:
                raise click.BadParameter(
                    f"Invalid keywords for type '{type_value}': {', '.join(invalid)}"
                )
        return value

    return validator


def find_value(key: str, keywords: dict, kwargs):
    """ """
    keywords_dict = dict(keywords)
    value = keywords_dict.get(key, kwargs.get(key))
    if value is None:
        raise click.BadParameter(f"{key}")
    return value
