# Параллельное моделирование теплопроводности на пластине методом Якоби.
#
# Сетка хранится как T[x, y]: y это номер строки пластины, x номер столбца.
# Julia хранит массивы по столбцам, поэтому одна строка пластины лежит в памяти
# подряд. Строки на каждой итерации делятся на блоки, блоки обновляются
# параллельно через Threads.@threads. Чтение идёт из одного массива, запись
# в другой, поэтому потоки друг другу не мешают.

using Printf

const USAGE = """
использование: julia -t <потоки> heat.jl N M верх низ лево право точность [файл.pgm]
  N, M      число строк и столбцов сетки (вместе с краями)
  верх ...  температуры на краях пластины
  точность  итерации идут, пока максимальное изменение за шаг не меньше её
пример: julia -t 8 heat.jl 200 300 100 0 0 0 1e-4 plate.pgm"""

const MAX_ITER = 1_000_000

"""Сетка с заданными краями и нулём внутри. Углы получают среднее двух краёв."""
function make_plate(N, M, top, bottom, left, right)
    T = zeros(Float64, M, N)
    T[:, 1] .= top
    T[:, N] .= bottom
    T[1, 2:N-1] .= left
    T[M, 2:N-1] .= right
    T[1, 1] = (top + left) / 2
    T[M, 1] = (top + right) / 2
    T[1, N] = (bottom + left) / 2
    T[M, N] = (bottom + right) / 2
    return T
end

"""Делит внутренние строки 2:N-1 на k почти равных блоков."""
function row_chunks(N, k)
    rows = 2:N-1
    k = min(k, length(rows))
    n = length(rows)
    return [rows[div((c - 1) * n, k)+1:div(c * n, k)] for c in 1:k]
end

"""Обновляет строки rows: dst = среднее четырёх соседей в src.
Возвращает максимальное изменение."""
function relax_rows!(dst, src, rows)
    M = size(src, 1)
    dmax = 0.0
    @inbounds for y in rows
        # ifelse вместо if, чтобы внутренний цикл векторизовался
        @simd for x in 2:M-1
            new = 0.25 * (src[x-1, y] + src[x+1, y] + src[x, y-1] + src[x, y+1])
            d = abs(new - src[x, y])
            dmax = ifelse(d > dmax, d, dmax)
            dst[x, y] = new
        end
    end
    return dmax
end

"""Одна итерация Якоби. Блоки строк обновляются параллельно, в diffs[c]
записывается максимальное изменение в блоке c. Один блок считается без
Threads.@threads, чтобы однопоточный замер не включал запуск задач."""
function jacobi_step!(dst, src, chunks, diffs)
    length(chunks) == 1 && return relax_rows!(dst, src, chunks[1])
    Threads.@threads :static for c in eachindex(chunks)
        diffs[c] = relax_rows!(dst, src, chunks[c])
    end
    return maximum(diffs)
end

"""Итерации до установления на k блоках строк (k параллельных задачах)."""
function solve(plate, eps, k)
    src = copy(plate)
    dst = copy(plate)  # края одинаковы в обоих массивах и не меняются
    chunks = row_chunks(size(plate, 2), k)
    diffs = zeros(length(chunks))
    iter = 0
    delta = Inf
    while delta >= eps && iter < MAX_ITER
        delta = jacobi_step!(dst, src, chunks, diffs)
        src, dst = dst, src
        iter += 1
    end
    return src, iter, delta
end

"""Сохраняет поле в двоичный PGM (P5): светлее значит горячее."""
function save_pgm(path, T)
    M, N = size(T)
    lo, hi = extrema(T)
    scale = hi > lo ? 255 / (hi - lo) : 0.0
    open(path, "w") do io
        write(io, "P5\n$M $N\n255\n")
        pixels = Vector{UInt8}(undef, M * N)
        i = 1
        for y in 1:N, x in 1:M
            pixels[i] = round(UInt8, (T[x, y] - lo) * scale)
            i += 1
        end
        write(io, pixels)
    end
end

function parse_args(args)
    length(args) in (7, 8) || return nothing
    try
        N = parse(Int, args[1])
        M = parse(Int, args[2])
        temps = parse.(Float64, args[3:6])
        eps = parse(Float64, args[7])
        out = length(args) == 8 ? args[8] : "plate.pgm"
        (N >= 3 && M >= 3 && eps > 0) || return nothing
        return (; N, M, temps, eps, out)
    catch
        return nothing
    end
end

function main(args)
    p = parse_args(args)
    if p === nothing
        println(stderr, USAGE)
        exit(1)
    end
    nt = Threads.nthreads()
    plate = make_plate(p.N, p.M, p.temps...)

    @printf("Сетка %d x %d, края: верх %g, низ %g, лево %g, право %g\n",
            p.N, p.M, p.temps...)
    @printf("Точность %g, потоков Julia: %d\n\n", p.eps, nt)

    # Прогрев: первая компиляция не должна попасть в замер.
    solve(make_plate(10, 10, p.temps...), p.eps, nt)

    counts = unique([[2^i for i in 0:floor(Int, log2(nt))]; nt])
    @printf("%8s  %10s  %12s  %10s\n", "потоков", "итераций", "время, с", "ускорение")
    result = nothing
    base = 0.0
    for k in counts
        t0 = time_ns()
        T, iter, delta = solve(plate, p.eps, k)
        t = (time_ns() - t0) / 1e9
        if result === nothing
            result = (T, iter, delta)
            base = t
        elseif T != result[1]
            println(stderr, "поле на $k потоках не совпало с однопоточным!")
            exit(1)
        end
        @printf("%8d  %10d  %12.3f  %10.2f\n", k, iter, t, base / t)
    end

    T, iter, delta = result
    println()
    if iter >= MAX_ITER
        @printf("Достигнут предел в %d итераций, последнее изменение %.3g\n", MAX_ITER, delta)
    else
        @printf("Установление за %d итераций, последнее изменение %.3g\n", iter, delta)
    end
    lo, hi = extrema(@view T[2:end-1, 2:end-1])
    @printf("Температура внутри: от %.3f до %.3f, в центре %.3f\n",
            lo, hi, T[(p.M + 1) ÷ 2, (p.N + 1) ÷ 2])
    save_pgm(p.out, T)
    println("Поле сохранено в $(p.out)")
end

main(ARGS)
