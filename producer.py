import sys
import argparse
import middleware
import random
import time

text = ["Lorem ipsum dolor sit amet.", "Non commodi molestiae At galisum aliquid sit corrupti maiores aut architecto ","quia ut dolore earum cum sunt totam."," Et consectetur odio sed numquam iste ut voluptatibus rerum ut velit ducimus sed labore necessitatibus?", "33 beatae unde sed eveniet nemo sit molestiae commodi?",

"Ea voluptatum enim non quod laboriosam ut internos sequi est neque nesciunt."," Et rerum ipsum est ipsum ullam ut odit nulla.",

"Aut similique incidunt qui consequuntur rerum eos laudantium perferendis ab Quis aliquam."," Et beatae dolorum qui illum dolores et tenetur sint in tenetur inventore sed amet expedita est consequuntur voluptatibus qui quod laborum.",]

class Producer:
    def __init__(self, datatype):
        self.type = datatype
        self.queue = [middleware.JSONQueue(f"/{self.type}", middleware.MiddlewareType.PRODUCER)]
        if datatype == "temperature":
            self.gen = self._temperature
        elif datatype == "msg":
            self.gen = self._msg
        elif datatype == "pressure":
            self.gen = self._pressure

    @classmethod
    def datatypes(self):
        return ["temperature", "msg", "pressure"]    

    def _temperature(self):
        time.sleep(0.1)
        yield random.randint(0,40)

    def _msg(self):
        time.sleep(0.2)
        yield random.choice(text)

    def _pressure(self):
        time.sleep(0.1)
        yield random.randint(0,40)

    def run(self, length=10):
        for _ in range(length):
            for queue, value in zip(self.queue, self.gen()):
                queue.push(value)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", help="type of producer: [temperature, msg, pressure]", default="temp")
    parser.add_argument("--length", help="number of messages to be sent", default=10)
    args = parser.parse_args()

    if args.type not in Producer.datatypes():
        print("Error: not a valid producer type")
        sys.exit(1)

    p = Producer(args.type)

    p.run(int(args.length))
