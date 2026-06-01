from __future__ import annotations

import logging
import struct
from typing import Sequence

# bge-small-en-v1.5 and all-MiniLM-L6-v2 are both 384-dim. The sqlite-vec
# virtual table dimension is fixed at creation, so swapping to a different-
# dim model (e.g. Voyage 1024) requires dropping news_event_vec manually.
EMBEDDING_DIM = 384


class EmbeddingProvider:
    """Wraps a sentence-transformer model. Loads lazily so the daemon can
    start up without paying the model-load cost until the first trigger
    actually needs to embed something."""

    def __init__(self, model_name: str, logger: logging.Logger):
        self.model_name = model_name
        self.logger = logger
        self._model = None

    def _ensure_loaded(self) -> None:
        if self._model is not None:
            return
        from sentence_transformers import SentenceTransformer

        self.logger.info("Loading embedding model: %s", self.model_name)
        self._model = SentenceTransformer(self.model_name)
        actual_dim = self._model.get_sentence_embedding_dimension()
        if actual_dim != EMBEDDING_DIM:
            raise RuntimeError(
                f"Embedding model {self.model_name} has dim {actual_dim}, "
                f"but news_event_vec is fixed at {EMBEDDING_DIM}. Either pick "
                f"a {EMBEDDING_DIM}-dim model or drop the news_event_vec table."
            )

    def embed(self, text: str) -> list[float]:
        self._ensure_loaded()
        # normalize_embeddings=True → unit vectors, so dot product == cosine sim
        vec = self._model.encode(text, normalize_embeddings=True)
        return vec.tolist()

    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        self._ensure_loaded()
        vecs = self._model.encode(list(texts), normalize_embeddings=True)
        return [v.tolist() for v in vecs]


def serialize_vector(vec: Sequence[float]) -> bytes:
    """Pack a float vector into the byte layout sqlite-vec expects."""
    return struct.pack(f"{len(vec)}f", *vec)
