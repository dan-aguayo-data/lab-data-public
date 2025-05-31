import random

fruits = ["Apple","Peach","Pear"]

#INDENTATION MARKS WHAT IS INSIDE THE FOR LOOP AND THE END OF A FOR LOOP
for fruit in fruits:
    print(fruit)
    print("inside the for loop")
print("out of the for loop")




#SUM
students_scores = [95,100,60,55,77,98,99,45,30,45,88]
total_Exam_score = sum(students_scores)
print(total_Exam_score)

x = 0
for score in students_scores:
    x += score

print(x)

#MAX NUMBER
z = 0
y = 0
for score in students_scores:
    z = score
    if z >= y:
        y = z
print(y)


#Range function creates a range of numbers to loop through
#range only works when you put inside another function
#range will not include the last number

for number in range(1,10):
    print(number)

test = 3 % 3
print(test)

for number in range(1,101):
    if number % 3 == 0 and number % 5 == 0:
        print("FizzBuzz")
    if number % 3 == 0:
        print("Fizz")
    if number % 5 == 0:
        print("Buzz")
    else:
        print(number)



letters = ['a','b','c','d','e']


print("Welcome to the password generator")

nr_letters = int(input("How many letters would you like in your password?\n"))

password = ""

for char in range(1,nr_letters +1):
    password += random.choice(letters)


print(password)

#in case you wanted to start a list to store teh choices and then shuffle them
password_list = []

#SHUFFLE THE ITEMS IN A LIST
fruits = ["Apple","Peach","Pear"]
print(fruits)


#shuffle function doest require you to assign this to a variable
random.shuffle(fruits)
print(fruits)
