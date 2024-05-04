const express = require("express");
const { PrismaClient } = require("@prisma/client");

const app = express();
const prisma = new PrismaClient();
const port = 3001;

app.get("/", (req, res) => {
  res.send("Hello");
});

app.listen(port, () => {
  console.log(`서버가 실행됩니다. http://localhost:${port}`);
});

app.get("/problems/1", async (req, res) => {
  const result = await prisma.customer.findMany({
    where: {
      income: {
        gte: 50000,
        lte: 60000,
      },
    },
    orderBy: [
      {
        income: "desc",
      },
      {
        lastName: "asc",
      },
      {
        firstName: "asc",
      },
    ],
    take: 10,
    select: {
      firstName: true,
      lastName: true,
      income: true,
    },
  });
  res.json(result);
});

app.get("/problems/2", async (req, res) => {
  const employees = await prisma.employee.findMany({
    where: {
      Branch_Employee_branchNumberToBranch: {
        branchName: {
          in: ["London", "Berlin"],
        },
      },
    },
    include: {
      Branch_Employee_branchNumberToBranch: {
        include: {
          Employee_Branch_managerSINToEmployee: true,
        },
      },
    },
  });
  const result = employees
    .map((employee) => ({
      sin: employee.sin,
      branchName: employee.Branch_Employee_branchNumberToBranch.branchName,
      salary: employee.salary,
      "Salary Diff": employee.Branch_Employee_branchNumberToBranch
        .Employee_Branch_managerSINToEmployee
        ? `${
            employee.Branch_Employee_branchNumberToBranch
              .Employee_Branch_managerSINToEmployee.salary - employee.salary
          }`
        : null,
    }))
    .sort((a, b) => b["Salary Diff"] - a["Salary Diff"])
    .slice(0, 10);
  res.json(result);
});

app.get("/problems/3", async (req, res) => {
  const butlerMaxIncome = await prisma.customer.findMany({
    where: {
      lastName: "Butler",
    },
    select: {
      income: true,
    },
  });

  let maxButlerIncome = 0;
  butlerMaxIncome.forEach((butler) => {
    if (butler.income && butler.income * 2 > maxButlerIncome) {
      maxButlerIncome = butler.income * 2;
    }
  });

  const result = await prisma.customer.findMany({
    where: {
      income: {
        gte: maxButlerIncome,
      },
    },
    orderBy: [{ lastName: "asc" }, { firstName: "asc" }],
    take: 10,
    select: {
      firstName: true,
      lastName: true,
      income: true,
    },
  });

  res.json(result);
});

app.get("/problems/4", async (req, res) => {
  const result = await prisma.employee.findMany({});
  res.json(result);
});

app.get("/problems/5", async (req, res) => {
  const result = await prisma.customer.findMany({});
  res.json(result);
});

app.get("/problems/6", async (req, res) => {
  const managerB = await prisma.employee.findFirst({
    where: {
      firstName: "Phillip",
      lastName: "Edwards",
    },
    select: {
      Branch_Branch_managerSINToEmployee: {
        select: {
          branchNumber: true,
        },
      },
    },
  });
  const branchN = managerB.Branch_Branch_managerSINToEmployee.branchNumber;

  const accounts = await prisma.account.findMany({
    where: {
      branchNumber: branchN,
    },
    orderBy: {
      accNumber: "asc",
    },
    select: {
      Branch: {
        select: {
          branchName: true,
        },
      },
      accNumber: true,
      balance: true,
    },
  });
  const filteredSortedAccounts = accounts
    .filter((account) => parseFloat(account.balance) > 100000)
    .sort((a, b) => parseInt(a.accNumber) - parseInt(b.accNumber))
    .slice(0, 10);
  res.json(
    filteredSortedAccounts.map((r) => ({
      branchName: r.Branch.branchName,
      accNumber: r.accNumber,
      balance: r.balance,
    }))
  );
});

app.get("/problems/7", async (req, res) => {
  const result = await prisma.employee.findMany({});
  res.json(result);
});

app.get("/problems/8", async (req, res) => {
  const employees = await prisma.employee.findMany({
    where: {
      salary: {
        gt: 50000,
      },
    },
    include: {
      Branch_Branch_managerSINToEmployee: true,
    },
    orderBy: [{ firstName: "asc" }],
  });
  const result = employees
    .map((employee) => ({
      sin: employee.sin,
      firstName: employee.firstName,
      lastName: employee.lastName,
      salary: employee.salary,
      branchName:
        employee.Branch_Branch_managerSINToEmployee.length > 0
          ? employee.Branch_Branch_managerSINToEmployee[0].branchName
          : null,
    }))
    .sort((a, b) => {
      if (a.branchName !== b.branchName) {
        return a.branchName === null
          ? 1
          : b.branchName === null
          ? -1
          : b.branchName.localeCompare(a.branchName);
      }
      return a.firstName.localeCompare(b.firstName);
    })
    .slice(0, 10);
  res.json(result);
});

app.get("/problems/9", async (req, res) => {
  const result = await prisma.employee.findMany({});
  res.json(result);
});

app.get("/problems/10", async (req, res) => {
  const result = await prisma.employee.findMany({});
  res.json(result);
});

app.get("/problems/11", async (req, res) => {
  const result = await prisma.employee.findMany({});
  res.json(result);
});

app.get("/problems/14", async (req, res) => {
  const sumOfSalaries = await prisma.employee.aggregate({
    _sum: {
      salary: true,
    },
    where: {
      Branch_Employee_branchNumberToBranch: {
        branchName: "Moscow",
      },
    },
  });

  const totalSalaries = sumOfSalaries._sum.salary;

  const result = {
    "sum of employees salaries": totalSalaries ? totalSalaries.toString() : "0",
  };
  res.json(result);
});

app.get("/problems/15", async (req, res) => {
  const result = await prisma.employee.findMany({});
  res.json(result);
});

app.get("/problems/17", async (req, res) => {
  const customers = await prisma.customer.findMany({
    where: {
      lastName: {
        startsWith: "S",
        contains: "e",
      },
    },
    include: {
      Owns: {
        include: {
          Account: true,
        },
      },
    },
  });

  const result = customers
    .filter(
      (customer) => new Set(customer.Owns.map((own) => own.accNumber)).size >= 3
    )
    .map((customer) => ({
      customerID: customer.customerID,
      firstName: customer.firstName,
      lastName: customer.lastName,
      income: customer.income,
      "average account balance": customer.Owns.reduce(
        (acc, current, _, array) =>
          acc + parseFloat(current.Account.balance) / array.length,
        0
      ),
    }))
    .sort((a, b) => a.customerID - b.customerID)
    .slice(0, 10);
  res.json(result);
});

app.get("/problems/18", async (req, res) => {
  const accounts = await prisma.account.findMany({
    where: {
      Branch: {
        branchName: "Berlin",
      },
    },
    include: {
      Transactions: true,
      Branch: true,
    },
  });

  const filteredAccounts = accounts
    .filter(
      (account) => account.Transactions && account.Transactions.length >= 10
    )
    .map((account) => ({
      accNumber: account.accNumber,
      balance: account.balance,
      sumOfTransactionAmounts: account.Transactions.reduce(
        (sum, transaction) => sum + parseFloat(transaction.amount),
        0
      ),
    }));

  const sortedAccounts = filteredAccounts.sort(
    (a, b) => a.sumOfTransactionAmounts - b.sumOfTransactionAmounts
  );

  const result = sortedAccounts.slice(0, 10);
  res.json(result);
});
