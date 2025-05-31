from question_model import Question
from data import question_data
from quiz_brain import QuizBrain


#TODO 2 Create Question Bank
question_bank = []
for question in question_data:
    question_text = question["text"]
    question_answer = question["answer"]
    new_question = Question(question_text,question_answer)
    question_bank.append(new_question)

print(question_bank)
print(question_bank[0].text)
print(question_bank[0].answer)


quiz_1 = QuizBrain(question_bank)

while quiz_1.still_has_questions():
    quiz_1.next_question()
print("You have completed the Quiz!")
print(f"Your final score is {quiz_1.score}/{len(question_bank)}")

