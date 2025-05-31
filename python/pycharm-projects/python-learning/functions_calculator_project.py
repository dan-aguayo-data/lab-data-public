import art
from art import text2art, art

def add(n1,n2):
    """this function will add"""
    return n1 + n2
def subtract(n1,n2):
    """this function will subtract"""
    return n1 - n2
def multiply(n1,n2):
    """this function will multiply"""
    return n1*n2
def divide(n1,n2):
    """this function will divide"""
    return n1/n2

#STORE FUNCTIONS INSIDE A DICTIONARY
operations ={
    "+": add,
    "-": subtract,
    "*": multiply,
    "/": divide
}


#USE DICTIONARY OPERATIONS TO PERFORM CALCULATIONS
print(operations["*"](2,3))

calculator_text = text2art("Calculator", font="block")
calculator_drawing = """
 _____________________
|  _________________  |
| |  PythonCalc    | |  
| |_________________| |  
|  ___ ___ ___   ___  |
| | 7 | 8 | 9 | | + | |
| |___|___|___| |___| |
| | 4 | 5 | 6 | | - | |
| |___|___|___| |___| |
| | 1 | 2 | 3 | | × | |
| |___|___|___| |___| |
| | . | 0 | = | | ÷ | |
| |___|___|___| |___| |
|_____________________|
"""


def calculator():
    print(f"{calculator_drawing}")
    should_accumulate_answer = True
    num1 = float(input("What is the first number?: "))
    while should_accumulate_answer:
        for symbol in operations:
            print(symbol)
        operation_symbol = input("Pick an operation from the above: ")
        num2 = float(input("What is the next number?: "))
        answer = operations[operation_symbol](num1,num2)
        print(f"{num1} + {num2} = {operations[operation_symbol](num1,num2)}")
        choice = input(f"Type 'y' to continue to work with number {answer}, or type 'n' to start new calculation: ")

        if choice == "y":
            num1 = answer
        else:
            should_accumulate_answer = False
            print("\n"*20)
            calculator() #this generates recursion inside the function

#this is the final calculator project being executed
calculator()




