

#describe the problem, untangle the problem

#reproduce the bug

#Play computer and evaluate each line

#Fix the errors, read error message.. read the last part of the error (start there)

#catch errors -- catch errros so the code doesn't crash

    # try:

    # except ValueError:
    #     print message

#use print statement


#use a debugger
# pythontutor.com
# thony editor

# debugging with Pycharm
#     click in the line of the code and create a breakpoint and go the top right into debug mode
#     a breakpoint do puts a break on the code at that particular line
#     step over in the console.. execute code line by linecache
#     step into will execute line by line... step into will go into the function definition and show how that works.. and step out
#     step into my code does the same thing as step into but ignores the library options...
#     you also have the console view in debugging you can see the prints


# Target is the number up to which we count
# Target is the number up to which we count
def fizz_buzz(target):
    target_adjusted = target + 1
    for number in range(1, target_adjusted):
        if number % 3 == 0 and number % 5 == 0:
            print("FizzBuzz")
        elif number % 3 == 0:
            print("Fizz")
        elif number % 5 == 0:
            print("Buzz")
        else:
            print([number])


fizz_buzz(30)









