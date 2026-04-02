from graphon.model_runtime.entities.common_entities import I18nObject
from graphon.model_runtime.utils.encoders import jsonable_encoder


def test_i18n_object_accepts_alias_keys_and_fills_missing_zh_hans():
    i18n = I18nObject.model_validate({"en_US": "Temperature"})

    assert i18n.en_us == "Temperature"
    assert i18n.zh_hans == "Temperature"


def test_i18n_object_serializes_with_locale_aliases():
    i18n = I18nObject(en_us="Temperature", zh_hans="温度")

    assert i18n.model_dump() == {"zh_Hans": "温度", "en_US": "Temperature"}
    assert jsonable_encoder(i18n) == {"zh_Hans": "温度", "en_US": "Temperature"}
