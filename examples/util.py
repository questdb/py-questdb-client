import random
from uuid import uuid1


def randint(low: int, high: int, mod: int = None) -> int:
    assert low < high
    value = random.randint(low, high)
    if mod:
        assert -1 < mod
        value = value % mod
    return value


def rand_table_name() -> str:
    return str(uuid1())


_SYMBOLS = ('ALPHA', 'BETA', 'OMEGA')


def rand_symbol() -> str:
    return random.choice(_SYMBOLS)


class Sensors:
    num_sensors = randint(5, 15)
    sensor_data = [randint(1, 100) for i in range(num_sensors)]

    def tick(self, num_ticks: int = 1):
        max_participants = 0
        for _ in range(num_ticks):
            if randint(1, 100) > 70:
                participants = randint(0, self.num_sensors)
                for _ in range(participants):
                    sensor_id = randint(0, self.num_sensors - 1)
                    self.sensor_data[sensor_id] += randint(-2, 2)
                if participants > max_participants:
                    max_participants = participants
        return max_participants

    def __getitem__(self, sensor_id: int) -> str:
        assert -1 < sensor_id < self.num_sensors
        return self.sensor_data[sensor_id]
