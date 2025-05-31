

#standard if statement
height = 122
if height > 120:   # the : starts the switch
    print("you can ride the rollercoaster") #with tab for when
else:  #indented as the if
    print("you can't ride") #with tab for when



# #comparison operators
# >
# <
# >=
# <=
#evaluates to true or false you can use  == instead of  =
#not equal to is !=

#MODULO OPERATOR => %
#It goes in between two number and it works out what is the remainder after the division
# 10 % 5 = 0
number_to_check = int(input("What is the number?"))

remainder = number_to_check % 2

if remainder == 0:
    print("it is even")
else:
    print("it is odd")


#NESTED CONDITIONAL you can use if twice and elif helps to establish mutliple when statements

print("Welcome!")

height = int(input("What is your height? again "))

if height > 120:
    print("you in")
    age = int(input("What is your age?"))
    if age <= 12:
        print("you pay $5")
    elif age <= 18:
        print("you pay $10")
    else:
        print("you pay $22")
else:
    print("you out")



#BMI Cals
weight = 72
height = 1.71

bmi = weight / (height ** 2)

print(f"bmi {bmi}")

if bmi < 18.5:
    print("underweight")
elif  bmi < 25:    #this works as a between...
    print("normal")
else:
    print("overweight")

#python will respect the order of the code, if you have a parent conditional you can add a do statement followed by nested and followed by another do statement


print("Welcome!")

height = int(input("What is your height? again again "))

if height > 120:
    print("you in")       #do statement of parent
    age = int(input("What is your age?"))   #do statement of parent
    if age <= 12:   #nested conditional
        bill = 5
        print("you pay $5")
    elif age <= 18:
        bill = - 10
        print("you pay $10")
    else:
        bill = 22
        print("you pay $22")
    wants_photo = input("Do you want to have a photo taken?")
    if wants_photo == "y":
        bill += 3
    print(f"your bill is ${bill}")   #do statement of parent
else:
    print("you out")


#LOGICAL OPERATORS
#A and B  -- Both have to be true
#C or D -- Can be either
#not E  -- not this

