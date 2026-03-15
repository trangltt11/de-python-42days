from __future__ import annotations

from dataclasses import dataclass
import random
import time
from typing import Callable, TypeVar, Iterable, Optional
import logging

T = TypeVar("T")


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 5          # tổng số lần thử (bao gồm lần đầu)
    base_delay_s: float = 0.2      # delay ban đầu
    max_delay_s: float = 5.0       # delay tối đa
    multiplier: float = 2.0        # hệ số tăng
    jitter_s: float = 0.1          # random thêm vào [0, jitter_s]


def retry_call(
    fn: Callable[[], T],
    *,
    config: RetryConfig = RetryConfig(),
    retry_on: tuple[type[BaseException], ...] = (TimeoutError, ConnectionError, OSError),
    logger: Optional[logging.Logger] = None,
    run_id: str = "",
    op: str = "operation",
) -> T:
    """
    Gọi fn() với retry + backoff.
    - Chỉ retry nếu exception thuộc retry_on.
    - Nếu hết attempts -> raise lỗi cuối cùng.
    """
    attempt = 1
    delay = config.base_delay_s

    while True:
        try:
            return fn()
        except retry_on as e:
            if attempt >= config.max_attempts:
                if logger:
                    logger.error(f"[run_id={run_id}] {op}: failed after {attempt} attempts: {type(e).__name__}: {e}")
                raise

            # log retry
            if logger:
                logger.warning(
                    f"[run_id={run_id}] {op}: attempt {attempt}/{config.max_attempts} failed "
                    f"({type(e).__name__}: {e}) -> retry in {delay:.2f}s"
                )

            # sleep với jitter
            sleep_s = min(config.max_delay_s, delay) + random.uniform(0, config.jitter_s)
            time.sleep(sleep_s)

            attempt += 1
            delay = min(config.max_delay_s, delay * config.multiplier)