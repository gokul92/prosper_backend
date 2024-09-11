import numpy as np
from scipy.optimize import minimize

class PortfolioOptimizer:
    def __init__(self, R, k, alpha, l, c, cash_index):
        self.R = R
        self.k = k
        self.alpha = alpha
        self.l = l
        self.c = c
        self.cash_index = cash_index

    def objective(self, x):
        Sigma = self.R.T @ self.R
        n = len(x)
        tracking_error = x.T @ Sigma @ x
        lasso_penalty = self.alpha * (1.0/n) * np.sum(np.abs(x))
        return tracking_error + lasso_penalty

    def constraint_sum(self, x):
        return np.sum(x)

    def constraint_cash(self, x):
        x_c = x[self.cash_index]
        return np.array([self.k[self.cash_index] - self.l - x_c, x_c - (self.k[self.cash_index] - (self.l + self.c))])

    def optimize(self):
        _, n = self.R.shape
        x0 = np.zeros(n)
        
        constraints = [
            {'type': 'eq', 'fun': self.constraint_sum},
            {'type': 'ineq', 'fun': self.constraint_cash}
        ]
        
        bounds = [(k_i - 1.0, k_i) for k_i in self.k]
        
        result = minimize(
            self.objective,
            x0,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints,
            options={'maxiter': 100, 'ftol': 1e-8, 'disp': True}
        )
        
        w = self.k - result.x
        return w, result

# Example usage
# T, n = 100, 5
# R = np.random.randn(T, n) * 0.01
# k = np.array([0.2, 0.2, 0.2, 0.2, 0.2])
# alpha, l, c = 1, 0.1, 0.005
# cash_index = 4

# optimizer = PortfolioOptimizer(R, k, alpha, l, c, cash_index)
# new_weights, optimization_result = optimizer.optimize()

# print("Optimization successful:", optimization_result.success)
# print("Optimization message:", optimization_result.message)
# print("New weights:", new_weights)
# print("Objective function value:", optimization_result.fun)

class TaxOptimizer:
    def __init__(self, delta, s, c, r, q, q_m):
        self.delta = delta  # Total number of shares to sell
        self.s = s          # Current share price
        self.c = c          # Cost basis for each tax lot
        self.r = r          # Tax rate for each tax lot
        self.q = q          # Total number of shares available
        self.q_m = q_m      # Number of shares in each tax lot
        self.a = len(q_m)   # Number of tax lots
        self.iteration = 0  # Counter for iterations

    def objective(self, f):
        return np.sum(f * self.q_m * (self.s - self.c) * self.r)/(self.delta * self.s)

    def constraint_sum(self, f):
        return np.sum(f * self.q_m) - self.delta

    def callback(self, xk):
        self.iteration += 1
        grad = self.q_m * (self.s - self.c) * self.r
        print(f"Iteration {self.iteration}:")
        print(f"  Current solution: {xk}")
        print(f"  Objective value: {self.objective(xk)}")

    def optimize(self):
        # Initial guess: sell proportionally from all lots
        f0 = self.delta * self.q_m / self.q / self.q_m

        constraints = [
            {'type': 'eq', 'fun': self.constraint_sum}
        ]

        bounds = [(0, 1) for _ in range(self.a)]

        result = minimize(
            self.objective,
            f0,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints,
            callback=self.callback,
            options={'ftol': 1e-12, 'maxiter': 1000, 'disp': True}
        )

        return result.x, result

# Example usage
# delta = 100  # Total shares to sell
# s = 25       # Current share price
# c = np.array([5, 7.5, 50, 85])  # Cost basis for each lot
# r = np.array([0.20, 0.20, 0.35, 0.35])  # Tax rates (long-term, long-term, short-term, short-term)
# q_m = np.array([3000, 1570, 350, 50])  # Shares in each lot
# q = np.sum(q_m)     # Total shares available

# optimizer = TaxOptimizer(delta, s, c, r, q, q_m)
# optimal_fractions, optimization_result = optimizer.optimize()

# print("Optimization successful:", optimization_result.success)
# print("Optimization message:", optimization_result.message)
# print("Optimal fractions to sell from each lot:", optimal_fractions)
# print("Total tax:", optimization_result.fun * delta * s)
# print("Shares sold from each lot:", optimal_fractions * q_m)
# print("Total shares sold:", np.sum(optimal_fractions * q_m))
