

#Python function
# = a function is a name followed by a set of parethesis


#making your own function your start with def....
#after def you give a function a name, parenthesis and double colon

def my_first_function():
    print("Hello")
    print("Bye")

#call the function
my_first_function()



#reeborg's world. Good to explain the functions.
# https://reeborg.ca/reeborg.html?lang=en&mode=python&menu=%2Fworlds%2Fmenus%2Fselect_collection_en.json&name=Other%20worlds&url=%2Fworlds%2Fmenus%2Fselect_collection_en.json

#jump challenge --jump 6 hurdles with robot having limited functions to use

# def turn_right():
#     turn_left()
#     turn_left()
#     turn_left()
#
# def jump():
#     move()
#     turn_left()
#     move()
#     turn_right()
#     move()
#     turn_right()
#     move()
#     turn_left()
#
# for step in range(6):
#     jump()
#

#Indentation = it is shifted to the right by 4 spaces. the lines in the same indentation are a block of code.

#Spaces vs tabs ----
# in python docs it says that spaces are preferred
# in python 3 you can't mix spaces and tabs
# in order to indent a line of code it should be done with 4 space bars
# code editors allow you to adapt tab key to 4 spaces


#while loop
#while x is true do this

#Example on how to use it wit hreborg hurdle challenge
# number_of_hurdles = 6
# while number_of_hurdles > 0:
#     jump()
#     number_of_hurdles-= 1

# you want to be using a while loop when you dont care of the number of the sequence
# while are more risky than for-loops, for-loops you set the range, for while-loops it could go to infinite if done
# wrong

#another example of reeborg
# def turn_right():
#     turn_left()
#     turn_left()
#     turn_left()
#
# def jump():
#     move()
#     turn_left()
#     move()
#     turn_right()
#     move()
#     turn_right()
#     move()
#     turn_left()

# while not at_goal():
#     if wall_in_front():
#         jump()
#     else:
#         move()


