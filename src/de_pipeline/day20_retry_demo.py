from __future__ import annotations

import logging
from .retry import retry_call, RetryConfig


class Flaky:
    def __init__(self, fail_times: int):
        self.fail_times = fail_times
        self.calls = 0

    def __call__(self) -> str:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise TimeoutError(f"simulated timeout at call={self.calls}")
        return "OK"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    logger = logging.getLogger("demo")

    flaky = Flaky(fail_times=2)

    result = retry_call(
        flaky,
        config=RetryConfig(max_attempts=5, base_delay_s=0.1, jitter_s=0.05),
        retry_on=(TimeoutError,),
        logger=logger,
        run_id="demo1234",
        op="fetch_api",
    )
    print("result:", result)


if __name__ == "__main__":
    main()