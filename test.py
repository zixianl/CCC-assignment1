N = 8
alpha = round(139.266798/ (139.266798 + 0.000503), 6)


T = 1181
print(((1-alpha) * T + (alpha * T ) / N))
a_speed = T / ((1-alpha) * T + (alpha * T ) / N)
print(a_speed)
print(alpha)



