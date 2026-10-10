from graphon.model_runtime.entities.provider_entities import (
    CredentialFormSchema,
    FormType,
    ProviderCredentialSchema,
)


def test_credential_form_schema_retains_help_from_manifest() -> None:
    """A manifest's per-field `help` must survive unmarshal.

    `dify-plugin-daemon#774` made the daemon return `help` on model-provider
    credential fields. Without an explicit field here, pydantic's default
    `extra="ignore"` discards the key and no consumer can ever see it.
    """
    schema = CredentialFormSchema.model_validate({
        "variable": "api_key",
        "label": {"en_US": "API Key"},
        "type": "secret-input",
        "help": {
            "en_US": "Get your API key from the provider console.",
            "zh_Hans": "从提供商控制台获取密钥。",
        },
        "required": True,
    })

    assert schema.help is not None
    assert schema.help.en_us == "Get your API key from the provider console."
    assert schema.help.zh_hans == "从提供商控制台获取密钥。"
    assert schema.model_dump()["help"]["en_US"] == (
        "Get your API key from the provider console."
    )


def test_credential_form_schema_help_is_optional() -> None:
    """A manifest that parses today must keep parsing unchanged."""
    schema = CredentialFormSchema.model_validate({
        "variable": "api_base",
        "label": {"en_US": "API Base"},
        "type": FormType.TEXT_INPUT,
    })

    assert schema.help is None


def test_provider_credential_schema_retains_nested_help() -> None:
    """`help` survives when nested inside a provider credential schema."""
    credential_schema = ProviderCredentialSchema.model_validate({
        "credential_form_schemas": [
            {
                "variable": "api_key",
                "label": {"en_US": "API Key"},
                "type": "secret-input",
                "help": {"en_US": "Found under Settings → API."},
            },
        ],
    })

    assert credential_schema.credential_form_schemas[0].help is not None
    assert credential_schema.credential_form_schemas[0].help.en_us == (
        "Found under Settings → API."
    )
