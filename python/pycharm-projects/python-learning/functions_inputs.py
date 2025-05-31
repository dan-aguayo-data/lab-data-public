# Define a function
def greet():
    print("Hello")
    print("How do you do?")
    print("Isn't the weather nice")
#Call a function
greet()

#Pass a variable to the function
def greet_specific(something):
    print(f"Hello {something}")
    print(f"How do you do {something}?")
    print(f"Isn't the weather nice?, {something}")

#Call function while passing variable
greet_specific("Angela")

#Naming conventions matter, here is how to refer to it
# something = parameter
# Angela = argument

def greet_with(name,location):
    print(f"Hello {name}!")
    print(f"What is it like in {location}?")

greet_with("Daniel","London")


#Keyword arguments == add each of the parameter names and the equal sign to assign an order

def greet_with_v2(name=1,location=2):
    print(f"Hello {name}!")
    print(f"What is it like in {location}")


greet_with_v2(name ="Sydney", location = "Fiona")
greet_with_v2(location = "Fiona", name ="Sydney")


