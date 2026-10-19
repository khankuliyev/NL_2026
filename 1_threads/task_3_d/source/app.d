/++
 Параллельная проверка гипотезы Коллатца.

 Диапазон 1..N делится на блоки, блоки обрабатываются параллельно
 через `parallel` из std.parallelism, частичные результаты объединяются
 через `TaskPool.reduce`.
+/
module app;

import core.time : MonoTime;
import std.algorithm : min;
import std.conv : to, ConvException;
import std.parallelism : TaskPool, totalCPUs;
import std.stdio : writeln, writefln, stderr;

enum size_t blockSize = 10_000;

/// Итог по отрезку чисел.
struct Result
{
    ulong longestStart = 1; /// число с самой длинной последовательностью
    uint longestSteps = 0;  /// число шагов до 1
    ulong peakStart = 1;    /// число с наибольшим промежуточным значением
    ulong peakValue = 1;    /// это значение
    ulong overflowAt = 0;   /// первое число, на котором случилось переполнение (0 если не было)
}

/// Строит последовательность для n. Возвращает false при переполнении 3n+1.
bool collatz(ulong n, out uint steps, out ulong peak)
{
    enum ulong limit = (ulong.max - 1) / 3;
    steps = 0;
    peak = n;
    while (n != 1)
    {
        if (n & 1)
        {
            if (n > limit)
                return false;
            n = 3 * n + 1;
            if (n > peak)
                peak = n;
        }
        else
            n >>= 1;
        ++steps;
    }
    return true;
}

/// Обрабатывает отрезок [from, to].
Result processRange(ulong from, ulong to)
{
    Result r;
    r.longestStart = from;
    r.peakStart = from;
    r.peakValue = from;
    foreach (n; from .. to + 1)
    {
        uint steps;
        ulong peak;
        if (!collatz(n, steps, peak))
        {
            if (r.overflowAt == 0)
                r.overflowAt = n;
            continue;
        }
        if (steps > r.longestSteps)
        {
            r.longestSteps = steps;
            r.longestStart = n;
        }
        if (peak > r.peakValue)
        {
            r.peakValue = peak;
            r.peakStart = n;
        }
    }
    return r;
}

/// Объединение двух частичных результатов. При равенстве берётся меньшее
/// число, поэтому итог не зависит от порядка объединения.
Result combine(Result a, Result b)
{
    Result r;
    if (a.longestSteps > b.longestSteps
            || (a.longestSteps == b.longestSteps && a.longestStart < b.longestStart))
    {
        r.longestSteps = a.longestSteps;
        r.longestStart = a.longestStart;
    }
    else
    {
        r.longestSteps = b.longestSteps;
        r.longestStart = b.longestStart;
    }
    if (a.peakValue > b.peakValue
            || (a.peakValue == b.peakValue && a.peakStart < b.peakStart))
    {
        r.peakValue = a.peakValue;
        r.peakStart = a.peakStart;
    }
    else
    {
        r.peakValue = b.peakValue;
        r.peakStart = b.peakStart;
    }
    if (a.overflowAt == 0)
        r.overflowAt = b.overflowAt;
    else if (b.overflowAt == 0)
        r.overflowAt = a.overflowAt;
    else
        r.overflowAt = min(a.overflowAt, b.overflowAt);
    return r;
}

/// Решает задачу на `threads` потоках.
Result solve(ulong n, uint threads)
{
    // Вызывающий поток тоже работает в parallel/reduce, поэтому рабочих на один меньше.
    auto pool = new TaskPool(threads - 1);
    scope (exit)
        pool.finish(true);

    immutable blocks = cast(size_t)((n + blockSize - 1) / blockSize);
    auto partial = new Result[blocks];
    // Блоки неравны по трудоёмкости, поэтому раздаём их по одному (workUnitSize = 1).
    foreach (i, ref r; pool.parallel(partial, 1))
    {
        immutable ulong from = cast(ulong) i * blockSize + 1;
        immutable ulong to = min(from + blockSize - 1, n);
        r = processRange(from, to);
    }
    return pool.reduce!combine(partial);
}

int main(string[] args)
{
    if (args.length != 2)
    {
        stderr.writeln("использование: collatz <N>");
        return 1;
    }
    ulong n;
    try
        n = args[1].to!ulong;
    catch (ConvException)
    {
        stderr.writeln("N должно быть натуральным числом");
        return 1;
    }
    if (n == 0)
    {
        stderr.writeln("N должно быть натуральным числом");
        return 1;
    }

    uint[] counts;
    for (uint t = 1; t < totalCPUs; t *= 2)
        counts ~= t;
    counts ~= totalCPUs;

    writefln("Проверка гипотезы Коллатца для чисел от 1 до %s", n);
    writefln("Ядер: %s, размер блока: %s чисел", totalCPUs, blockSize);
    writeln();

    Result reference;
    double baseTime = 0;
    writefln("%8s  %12s  %10s", "потоков", "время, мс", "ускорение");
    foreach (idx, t; counts)
    {
        auto start = MonoTime.currTime;
        auto r = solve(n, t);
        double ms = (MonoTime.currTime - start).total!"usecs" / 1000.0;
        if (idx == 0)
        {
            reference = r;
            baseTime = ms;
        }
        else if (r != reference)
        {
            stderr.writefln("результат на %s потоках не совпал с однопоточным!", t);
            return 1;
        }
        writefln("%8s  %12.2f  %10.2f", t, ms, baseTime / ms);
    }

    writeln();
    writefln("Самая длинная последовательность: у числа %s, %s шагов (%s членов)",
            reference.longestStart, reference.longestSteps, reference.longestSteps + 1);
    writefln("Наибольшее промежуточное значение: %s, получено из числа %s",
            reference.peakValue, reference.peakStart);
    if (reference.overflowAt != 0)
        writefln("Внимание: переполнение ulong, первое такое число %s (оно пропущено)",
                reference.overflowAt);
    else
        writeln("Переполнений ulong не было, все последовательности дошли до 1");
    return 0;
}
