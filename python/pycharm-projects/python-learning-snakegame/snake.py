from turtle import Turtle

MOVE_DISTANCE = 20
UP = 90
DOWN = 270
LEFT = 180
RIGHT = 0

class Snake:
    def __init__(self):
        self.segments = []
        self.create_snake()
        self.head = self.segments[0]

    def create_snake(self):
        for n in range(3):
            name = f'segment_{n + 1}'
            name = Turtle(shape="square")
            name.color("white")
            name.penup()
            name.goto(n * -20, 0)
            self.segments.append(name)

    def move(self):
        for seg_num in range(len(self.segments) - 1, 0, -1):  # start,stop,step
            new_x = self.segments[seg_num - 1].xcor()
            new_y = self.segments[seg_num - 1].ycor()
            self.segments[seg_num].goto(new_x, new_y)
        self.head.forward(MOVE_DISTANCE)

    def add_segment(self,position, seg_num):
        name = f'segment_{seg_num + 1}'
        name = Turtle(shape="square")
        name.color("white")
        name.penup()
        name.goto(seg_num * -20, 0)
        self.segments.append(name)

    def extend(self):
        self.add_segment(self.segments[-1].position(),len(self.segments))


    def up(self):
        if self.head.heading() != DOWN:
            self.head.setheading(UP)

    def down(self):
        if self.head.heading() != UP:
            self.head.setheading(DOWN)

    def left(self):
        if self.head.heading() != RIGHT:
            self.head.setheading(LEFT)

    def right(self):
        if self.head.heading() != LEFT:
            self.head.setheading(RIGHT)



