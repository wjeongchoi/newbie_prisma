const express = require("express");
const { PrismaClient } = require("@prisma/client");

const app = express();
app.use(express.json());
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
  const londonCustomers = await prisma.customer.findMany({
    where: {
      income: {
        gt: 80000,
      },
      Owns: {
        some: {
          Account: {
            Branch: {
              branchName: "London",
            },
          },
        },
      },
    },
    select: {
      customerID: true,
      income: true,
      Owns: {
        select: {
          accNumber: true,
          Account: {
            select: {
              branchNumber: true,
            },
          },
        },
      },
    },
  });

  const latveriaCustomers = await prisma.customer.findMany({
    where: {
      income: {
        gt: 80000,
      },
      Owns: {
        some: {
          Account: {
            Branch: {
              branchName: "Latveria",
            },
          },
        },
      },
    },
    select: {
      customerID: true,
      income: true,
      Owns: {
        select: {
          accNumber: true,
          Account: {
            select: {
              branchNumber: true,
            },
          },
        },
      },
    },
  });

  const intersection = londonCustomers.filter((customer) => {
    return latveriaCustomers.some(
      (latveriaCustomer) => latveriaCustomer.customerID === customer.customerID
    );
  });
  const filteredIntersection = intersection.filter((customer) => {
    return customer.income > 80000;
  });
  const formattedIntersection = filteredIntersection.flatMap((customer) => {
    return customer.Owns.map((own) => {
      return {
        customerID: customer.customerID,
        income: customer.income,
        accNumber: own.accNumber,
        branchNumber: own.Account.branchNumber,
      };
    });
  });

  const sortedIntersection = formattedIntersection.sort((a, b) => {
    if (a.customerID !== b.customerID) {
      return a.customerID - b.customerID;
    } else {
      return a.accNumber - b.accNumber;
    }
  });

  res.json(sortedIntersection.slice(0, 10));
});

app.get("/problems/5", async (req, res) => {
  const accounts = await prisma.account.findMany({
    where: {
      type: {
        in: ["BUS", "SAV"],
      },
    },
    select: {
      accNumber: true,
      type: true,
      balance: true,
      Owns: {
        select: {
          customerID: true,
        },
      },
    },
  });
  const results = accounts.flatMap((account) =>
    account.Owns.map((owns) => ({
      customerID: owns.customerID,
      type: account.type,
      "account number": account.accNumber,
      balance: account.balance,
    }))
  );
  results.sort((a, b) => {
    if (a.customerID !== b.customerID) {
      return a.customerID - b.customerID;
    } else if (a.type !== b.type) {
      return a.type.localeCompare(b.type);
    } else {
      return a.accNumber - b.accNumber;
    }
  });
  res.json(results.slice(0, 10));
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
  const newYorkBranch = await prisma.branch.findFirst({
    where: {
      branchName: "New York",
    },
  });
  const londonBranch = await prisma.branch.findFirst({
    where: {
      branchName: "London",
    },
  });
  const londonBranchNumber = londonBranch?.branchNumber;
  const newYorkBranchNumber = newYorkBranch?.branchNumber;
  const newYorkCustomers = await prisma.customer.findMany({
    where: {
      Owns: {
        some: {
          Account: {
            branchNumber: newYorkBranchNumber,
          },
        },
      },
    },
    select: {
      customerID: true,
      income: true,
      Owns: {
        select: {
          accNumber: true,
          Account: {
            select: {
              branchNumber: true,
            },
          },
        },
      },
    },
  });

  const londonCustomers = await prisma.customer.findMany({
    where: {
      Owns: {
        some: {
          Account: {
            branchNumber: londonBranchNumber,
          },
        },
      },
    },
    select: {
      customerID: true,
      income: true,
      Owns: {
        select: {
          accNumber: true,
          Account: {
            select: {
              branchNumber: true,
            },
          },
        },
      },
    },
  });
  const newYorkCustomerIDs = newYorkCustomers.map(
    (customer) => customer.customerID
  );
  const londonCustomerIDs = londonCustomers.map(
    (customer) => customer.customerID
  );

  const customersNotInLondon = await prisma.customer.findMany({
    where: {
      AND: [
        {
          customerID: {
            in: newYorkCustomerIDs,
          },
        },
        {
          NOT: {
            customerID: {
              in: londonCustomerIDs,
            },
          },
        },
      ],
    },
    select: {
      customerID: true,
      income: true,
      Owns: {
        select: {
          accNumber: true,
          Account: {
            select: {
              branchNumber: true,
            },
          },
        },
      },
    },
  });

  const londonCustomerAccounts = await prisma.customer.findMany({
    where: {
      customerID: {
        in: londonCustomerIDs,
      },
    },
    select: {
      customerID: true,
      income: true,
      Owns: {
        select: {
          accNumber: true,
          Account: {
            select: {
              branchNumber: true,
            },
          },
        },
      },
    },
  });
  const londonAccountNumbers = londonCustomerAccounts.flatMap((customer) => {
    return customer.Owns.map((own) => own.accNumber);
  });

  const customersWithLondonAccounts = await prisma.customer.findMany({
    where: {
      Owns: {
        some: {
          accNumber: {
            in: londonAccountNumbers,
          },
        },
      },
    },
    select: {
      customerID: true,
      income: true,
      Owns: {
        select: {
          accNumber: true,
          Account: {
            select: {
              branchNumber: true,
            },
          },
        },
      },
    },
  });

  const customersNotInLondonIDs = customersNotInLondon.map(
    (customer) => customer.customerID
  );
  const customersWithLondonAccountsIDs = customersWithLondonAccounts.map(
    (customer) => customer.customerID
  );

  const uniqueCustomersWithLondonAccountsIDs = new Set(
    customersWithLondonAccountsIDs
  );
  const filteredCustomersNotInLondonIDs = customersNotInLondonIDs.filter(
    (customerID) => !uniqueCustomersWithLondonAccountsIDs.has(customerID)
  );

  const customersNotInLondonOnlySorted = filteredCustomersNotInLondonIDs
    .map((customerID) => {
      return {
        customerID: customerID,
      };
    })
    .sort((a, b) => a.customerID - b.customerID);

  res.json(customersNotInLondonOnlySorted.slice(0, 10));
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
  const employees = await prisma.employee.findMany({
    where: {
      salary: {
        gt: 50000,
      },
    },
  });
  const employeeDetails = await Promise.all(
    employees.map(async (employee) => {
      const managedBranch = await prisma.branch.findFirst({
        where: {
          managerSIN: employee.sin,
        },
        select: {
          branchName: true,
        },
      });
      return {
        sin: employee.sin,
        firstName: employee.firstName,
        lastName: employee.lastName,
        salary: employee.salary,
        branchName: managedBranch ? managedBranch.branchName : null,
      };
    })
  );
  const result = employeeDetails
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

app.get("/problems/10", async (req, res) => {
  const helenBranches = await prisma.owns.findMany({
    where: {
      Customer: {
        firstName: "Helen",
        lastName: "Morgan",
      },
    },
    include: {
      Account: true,
    },
  });
  const helenBranchNumbers = new Set(
    helenBranches.map((own) => own.Account.branchNumber)
  );

  const potentialCustomers = await prisma.customer.findMany({
    where: {
      income: {
        gt: 5000,
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

  const qualifiedCustomers = potentialCustomers.filter((customer) => {
    const customerBranchNumbers = new Set(
      customer.Owns.map((own) => own.Account.branchNumber)
    );
    return [...helenBranchNumbers].every((branch) =>
      customerBranchNumbers.has(branch)
    );
  });

  qualifiedCustomers.sort((a, b) => b.income - a.income);

  const result = qualifiedCustomers.slice(0, 10).map((customer) => ({
    customerID: customer.customerID,
    firstName: customer.firstName,
    lastName: customer.lastName,
    income: customer.income,
  }));
  res.json(result);
});

app.get("/problems/11", async (req, res) => {
  const minSalaryData = await prisma.employee.findMany({
    where: {
      Branch_Employee_branchNumberToBranch: {
        branchName: "Berlin",
      },
    },
    orderBy: {
      salary: "asc",
    },
    take: 1,
  });

  const lowestSalary = minSalaryData[0].salary;

  const employees = await prisma.employee.findMany({
    where: {
      salary: lowestSalary,
      Branch_Employee_branchNumberToBranch: {
        branchName: "Berlin",
      },
    },
    orderBy: {
      sin: "asc",
    },
    take: 10,
  });

  const result = employees.map((emp) => ({
    sin: emp.sin,
    firstName: emp.firstName,
    lastName: emp.lastName,
    salary: emp.salary,
  }));
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
  const customers = await prisma.customer.findMany({
    include: {
      Owns: {
        include: {
          Account: {
            include: {
              Branch: true,
            },
          },
        },
      },
    },
  });

  const result = customers
    .filter((customer) => {
      const branchNames = new Set(
        customer.Owns.map((own) => own.Account.Branch.branchName)
      );
      return branchNames.size === 4;
    })
    .map((customer) => ({
      customerID: customer.customerID,
      firstName: customer.firstName,
      lastName: customer.lastName,
    }))
    .sort((a, b) => {
      const lastNameComparison = a.lastName.localeCompare(b.lastName);
      if (lastNameComparison !== 0) return lastNameComparison;
      return a.firstName.localeCompare(b.firstName);
    })
    .slice(0, 10); // Limit to top 10
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

app.post("/employee/join", async (req, res) => {
  try {
    const { sin, firstName, lastName, salary, branchNumber } = req.body;

    const newEmployee = await prisma.employee.create({
      data: {
        sin: sin,
        firstName: firstName,
        lastName: lastName,
        salary: salary,
        branchNumber: branchNumber,
      },
    });

    res.status(201)
      .send(`'이 팀은 미친듯이 일하는 일꾼들로 이루어진 광전사 설탕 노움 조합이다.
      분위기에 적응하기는 쉽지 않지만 아주 화력이 좋은 강력한 조합인거 같다.'`);
  } catch (error) {
    console.error("Error adding new employee: ", error);
    res.status(500).send("Failed to add new employee.");
  }
});

app.delete("/employee/leave", async (req, res) => {
  const { sin } = req.body;

  if (!sin) {
    return res.status(400).send({ message: "Missing employee SIN number" });
  }
  try {
    const sinNumber = parseInt(sin);

    const employeeExists = await prisma.employee.findUnique({
      where: { sin: sinNumber },
    });

    if (!employeeExists) {
      return res.status(404).send({ message: "Employee not found" });
    }

    await prisma.employee.delete({
      where: { sin: sinNumber },
    });

    res
      .status(200)
      .send(
        "안녕히 계세요 여러분! \n전 이 세상의 모든 굴레와 속박을 벗어 던지고 제 행복을 찾아 떠납니다!\n여러분도 행복하세요~~!"
      );
  } catch (error) {
    res.status(500).send({
      message: "An error occurred while deleting the employee",
      error: error.message,
    });
  }
});

app.post("/account/:account_no/deposit", async (req, res) => {
  const { account_no } = req.params;
  const { customerID, amount } = req.body;

  if (!amount || amount <= 0) {
    return res.status(400).send("Invalid deposit amount.");
  }

  try {
    const accountNumber = parseInt(account_no);

    const account = await prisma.account.findUnique({
      where: { accNumber: accountNumber },
      include: { Owns: true },
    });

    if (!account) {
      return res.status(404).send("Account not found.");
    }

    const isOwner = account.Owns.some(
      (ownership) => ownership.customerID === customerID
    );
    if (!isOwner) {
      return res
        .status(403)
        .send("Unauthorized: Customer does not own this account.");
    }

    const newBalance = parseFloat(account.balance) + parseFloat(amount);

    const updatedAccount = await prisma.account.update({
      where: { accNumber: accountNumber },
      data: { balance: newBalance.toString() },
      select: { balance: true },
    });

    res.status(200).send(updatedAccount.balance);
  } catch (error) {
    res.status(500).send({
      message: "An error occurred while processing your request",
      error: error.message,
    });
  }
});

app.post("/account/:account_no/withdraw", async (req, res) => {
  const { account_no } = req.params;
  const { customerID, amount } = req.body;

  if (!amount || amount <= 0) {
    return res.status(400).send("Invalid withdrawal amount.");
  }

  try {
    const accountNumber = parseInt(account_no);

    const account = await prisma.account.findUnique({
      where: { accNumber: accountNumber },
      include: { Owns: true },
    });

    if (!account) {
      return res.status(404).send("Account not found.");
    }

    const isOwner = account.Owns.some(
      (ownership) => ownership.customerID === customerID
    );
    if (!isOwner) {
      return res
        .status(403)
        .send("Unauthorized: Customer does not own this account.");
    }

    const currentBalance = parseFloat(account.balance);
    if (amount > currentBalance) {
      return res.status(400).send("Insufficient funds for withdrawal.");
    }

    const newBalance = currentBalance - amount;

    const updatedAccount = await prisma.account.update({
      where: { accNumber: accountNumber },
      data: { balance: newBalance.toString() },
      select: { balance: true },
    });

    res.status(200).send(updatedAccount.balance);
  } catch (error) {
    res.status(500).send({
      message: "An error occurred while processing your request",
      error: error.message,
    });
  }
});
