# Prefix	Purpose	Example
# ""	Plain string	"Hello"
# r""	Raw string (no escape sequences)	r"C:\path\to\file"
# f""	Formatted string (variable interpolation)	f"Hello, {name}"
# b""	Byte string	b"Hello"
# u""	Unicode string (redundant in Python 3)	u"Hello"

def format_name(f_name,l_name):
    """docstring example for function documentation
        this docstring can be multi-line"""
    formated_f_name = f_name.title()
    formated_l_name = l_name.title()
    return f'{formated_f_name} {formated_l_name}'


formated_string = format_name('Daniel', 'Aguayo')
print(formated_string)
print(format_name('Daniel', 'Aguayo'))

#multiple return statements
#return workd says to the function is the end of the function


def format_name(f_name,l_name):
    if f_name == "" or l_name == "":
        return "You did not provide values"
    formated_f_name = f_name.title()
    formated_l_name = l_name.title()
    return f'{formated_f_name} {formated_l_name}'



formated_string = format_name(input("What is your first name? "), input("What is your last name?"))
print(formated_string)
print(format_name('Daniel', 'Aguayo'))


#A way to change the name of functions

new_name_example = format_name    #parenthesis is what triggersa function so we avoid it in here

new_name_example("Daniel", "Aguayo")  #function with new name


