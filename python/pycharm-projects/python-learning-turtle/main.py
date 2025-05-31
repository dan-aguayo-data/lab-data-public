import turtle
import another_module
import prettytable

print(another_module.another_variable)  #getting a variable


timmy = turtle.Turtle()    #getting a class.. this is denoted with a capital letter at the start

print(timmy)
timmy.shape("turtle")
timmy.color("coral")
timmy.forward(100)

#Another way of importing
# from turtle import Turtle
# timmy = Turtle()

my_screen = turtle.Screen()  #object
print(my_screen.canvheight)  #attribute

my_screen.exitonclick()

table = prettytable.PrettyTable()


table.add_column("Pokemon Name",["Pikachu","Squirrel","Charmander"])
table.add_column("Type",["Electric","Water","Fire"])

table.align = 'l'

print(table)