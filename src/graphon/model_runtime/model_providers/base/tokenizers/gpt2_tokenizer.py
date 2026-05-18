import logging
from abc import abstractmethod
from collections.abc import Sequence
from pathlib import Path
from threading import Lock
from typing import Protocol

logger = logging.getLogger(__name__)


class _TokenizerProtocol(Protocol):
    @abstractmethod
    def encode(self, text: str) -> Sequence[int]: ...


_tokenizer: _TokenizerProtocol | None = None
_lock = Lock()


def _try_load_tiktoken_encoder() -> _TokenizerProtocol | None:
    try:
        import tiktoken  # noqa: PLC0415

        return tiktoken.get_encoding("gpt2")
    except Exception:
        logger.debug(
            "Failed to initialize tiktoken GPT-2 tokenizer; falling back",
            exc_info=True,
        )
        return None


class GPT2Tokenizer:
    @staticmethod
    def _get_num_tokens_by_gpt2(text: str) -> int:
        """Use gpt2 tokenizer to get num tokens"""
        tokenizer = GPT2Tokenizer.get_encoder()
        tokens = tokenizer.encode(text)
        return len(tokens)

    @staticmethod
    def get_num_tokens(text: str) -> int:
        return GPT2Tokenizer._get_num_tokens_by_gpt2(text)

    @staticmethod
    def get_encoder() -> _TokenizerProtocol:
        global _tokenizer  # noqa: PLW0603
        if _tokenizer is not None:
            return _tokenizer
        with _lock:
            if _tokenizer is None:
                # Try to use tiktoken to get the tokenizer because it is faster
                _tokenizer = _try_load_tiktoken_encoder()
                if _tokenizer is None:
                    import transformers  # noqa: PLC0415

                    gpt2_tokenizer_path = Path(__file__).resolve().parent / "gpt2"
                    _tokenizer = transformers.GPT2Tokenizer.from_pretrained(
                        str(gpt2_tokenizer_path),
                    )
                    logger.info(
                        "Fallback to Transformers' GPT-2 tokenizer from tiktoken",
                    )

            return _tokenizer
