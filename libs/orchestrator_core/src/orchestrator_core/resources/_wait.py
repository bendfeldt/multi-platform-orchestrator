import time
from collections.abc import Callable


def wait_until_done[T](
    poll: Callable[[], T],
    is_done: Callable[[T], bool],
    cancel: Callable[[], object],
    interval: float,
) -> T:
    """Polls a remote job until it is done; cancels it if the Dagster run stops first.

    A terminated Dagster run interrupts this loop (KeyboardInterrupt or a Dagster
    interrupt error), so the remote job must be cancelled here or it keeps running.
    """
    try:
        while not is_done(state := poll()):
            time.sleep(interval)
        return state
    except BaseException:
        cancel()
        raise
