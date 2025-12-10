from datetime import datetime, timedelta

class CircuitBreaker:
    def __init__(self, failure_threshold, recovery_timeout):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failures = 0
        self.last_failure_time = None
        self.state = 'CLOSED'

    def call(self, func, args, **kwargs):
        if self.state == 'OPEN':
            if datetime.now() >= self.last_failure_time + timedelta(seconds=self.recovery_timeout):
                self.state = 'HALF-OPEN'
            else:
                raise Exception("Circuit is open. Please try again later.")

        try:
            result = func(args, **kwargs)
            self.reset()
            return result
        except Exception as e:
            self.record_failure()
            raise e

    def reset(self):
        self.failures = 0
        self.state = 'CLOSED'

    def record_failure(self):
        self.failures += 1
        self.last_failure_time = datetime.now()
        if self.failures >= self.failure_threshold:
            self.state = 'OPEN'