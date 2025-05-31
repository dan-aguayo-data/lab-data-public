from turtle import Turtle, Screen

tim = Turtle()
screen = Screen()


def move_forward():
    tim.forward(10)


screen.listen()
screen.onkey(key='space',fun=move_forward)  #dont put the parenthesis on the function, pass function as input
screen.exitonclick()



