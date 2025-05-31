from turtle import Turtle, Screen

tim = Turtle()
tim.shape("turtle")
tim.color("blue")



screen = Screen()

def move_forward():
    tim.forward(10)
def move_backward():
    tim.backward(10)
def move_right():
    tim.right(10)
def move_left():
    tim.left(10)

screen.listen()
screen.onkey(move_forward, "w")
screen.onkey(move_backward, "s")
screen.onkey(move_right, "d")
screen.onkey(move_left, "a")
screen.onkey(screen.reset,"c")


screen.exitonclick()