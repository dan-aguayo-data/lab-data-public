import turtle
from turtle import Turtle, Screen
import random

is_race_on = False
screen = Screen()
screen.setup(width=500,height=400)

user_bet = screen.textinput("Make your bet","Which turtle will win the race? \nYour options are: red,orange,yellow,green,blue,purple. \nEnter a colour:")
colors = ["red","orange","yellow","green","blue","purple"]

n = 0
turtle_name = f"tim_{n}"
x_start = -230
y_start = -110
all_turtles = []
for color in colors:
     n += 1
     y_start += 30
     turtle_name = Turtle(shape="turtle")
     turtle_name.penup()
     turtle_name.color(colors[n-1])
     turtle_name.goto(x_start,y_start)
     all_turtles.append(turtle_name)

if user_bet:
    is_race_on = True

while is_race_on:
    for turtle in all_turtles:
        if turtle.xcor() > 230:
            is_race_on = False
            winning_color = turtle.pencolor()
            if str(winning_color).upper() == str(user_bet).upper():
                print(f"You won, the turtle's winning colour is {winning_color}")
            else:
                print(f"You have lost, the turtle's winning colour is {winning_color}")

        random_distance = random.randint(1,10)
        turtle.forward(random_distance)



screen.exitonclick()