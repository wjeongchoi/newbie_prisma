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
  const result = await prisma.employee.findMany({});
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
