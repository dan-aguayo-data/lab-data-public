from turtle import Turtle
from turtle import Screen
from turtle import colormode as f_colormode
import random


tim = Turtle()
f_colormode(255)

def rgb_random_color():
    r = random.randint(0,255)
    g = random.randint(0,255)
    b = random.randint(0, 255)
    random_color = (r,g,b)
    return random_color
#rgb colours from 0 to 255 in a tuple for r,g,b
# colours = ["CornflowerBlue", "DarkOrchid", "IndianRed", "DeepSkyBlue", "LightSeaGreen", "wheat", "SlateGray", "SeaGreen"]
directions = [0,90,180,270]
#tim.pensize(15)
tim.speed(0)

def draw_spirograph(size_of_gap):
    for _ in range(int(360/size_of_gap)):
        tim.color(rgb_random_color())
        tim.circle(100)
        tim.setheading(tim.heading() + size_of_gap)

draw_spirograph(5)



# for _ in range(200):
#     tim.color(rgb_random_color())
#     tim.forward(30)
#     tim.setheading(random.choice(directions))

#
# def draw_shape(num_sides):
#     angle = 360 / num_sides
#     for _ in range(num_sides):
#         tim.forward(100)
#         tim.right(angle)
#
# for x in range(3,11):
#     tim.color(random.choice(colours))
#     draw_shape(x)






screen = Screen()
screen.exitonclick()


