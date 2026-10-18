# Resilience Toolkit

## Задание

Любой сервис рано или поздно начинает ломаться: внешний API отвечает ошибками, база тормозит, клиент присылает
в сто раз больше запросов, чем вы можете обработать. Чтобы одна поломка не положила всё остальное, вызовы «опасных»
операций оборачивают в **политики отказоустойчивости**.

Напишите небольшую библиотеку таких политик в пакете [`internal/resilience`](./internal/resilience).
Все политики реализуют один интерфейс и вкладываются друг в друга:

```go
type Policy interface {
	Do(op func() error) error
}
```

```go
limiter, _ := resilience.NewTokenBucket(clock, 10, 100*time.Millisecond)
breaker, _ := resilience.NewCircuitBreaker(clock, resilience.Config{
	FailureThreshold:  5,
	OpenTimeout:       30 * time.Second,
	HalfOpenSuccesses: 2,
})
policy, _ := resilience.Chain(limiter, breaker)

err := policy.Do(func() error { return callExternalService() })
```

Задание состоит из базовой части (разделы 1–3) и бонусной (разделы 4–8). Бонусная часть по желанию.

Файлы `clock.go`, `errors.go` и `policy.go` готовы, менять их не нужно. В остальных файлах лежат заготовки
с `panic("not implemented")`. Добавлять поля и вспомогательные типы можно, менять сигнатуры экспортируемых
функций и методов нельзя.

## Правила

- Разрешены импорты `errors`, `fmt`, `iter`, `math`, `slices`, `sync` и `time` (из `time` только типы и константы).
  Пакеты `sort`, `container/*` и сторонние библиотеки запрещены.
- **Нельзя вызывать `time.Now()` и `time.Sleep()`.** Время берётся только из `Clock`, который передаётся
  в конструктор. Поэтому тесты работают мгновенно: они подставляют `FakeClock`.
- Конструкторы не паникуют. При некорректных аргументах они возвращают ошибку из `errors.go`
  (для каждой проблемы своя).

## Базовая часть

### 1. TokenBucket

```go
func NewTokenBucket(clock Clock, capacity int, interval time.Duration) (*TokenBucket, error)
func (b *TokenBucket) Allow() bool
func (b *TokenBucket) Tokens() int
func (b *TokenBucket) Do(op func() error) error
```

Ограничитель частоты. В «ведре» помещается не больше `capacity` токенов, оно создаётся полным, а новый токен
появляется раз в `interval`. Вызов операции стоит один токен.

- `Allow` забирает токен, если он есть.
- `Tokens` показывает, сколько токенов доступно.
- `Do` выполняет `op`, если токен есть, иначе возвращает `ErrRateLimited`.

### 2. CircuitBreaker

```go
type State int // Closed, Open, HalfOpen

type Config struct {
	FailureThreshold  int
	OpenTimeout       time.Duration
	HalfOpenSuccesses int
	IsFailure         func(error) bool
}

func NewCircuitBreaker(clock Clock, cfg Config) (*CircuitBreaker, error)
func (c *CircuitBreaker) State() State
func (c *CircuitBreaker) Do(op func() error) error
```

«Предохранитель»: если сервис упал, он на время перестаёт вызывать операцию и сразу отвечает `ErrCircuitOpen`.

```
 ┌────────┐  FailureThreshold ошибок подряд   ┌──────┐
 │ Closed │ ────────────────────────────────► │ Open │
 └────────┘                                   └──────┘
      ▲                                          │ прошло OpenTimeout
      │ HalfOpenSuccesses успешных проб          ▼
      │                                     ┌──────────┐
      └──────────────────────────────────── │ HalfOpen │ ── ошибка ──► Open
                                            └──────────┘
```

- `IsFailure` решает, считать ли ошибку отказом. Если он `nil`, отказом считается любая ошибка.
- У `State` есть метод `String()`, он возвращает `"closed"`, `"open"` и `"half-open"`.

### 3. Chain

```go
func Chain(policies ...Policy) (Policy, error)
```

Склеивает политики в одну. Первая политика самая внешняя, последняя ближе всего к операции.

## Бонусная часть

### 4. Backoff и Retry

```go
type Backoff interface {
	Delay(attempt int) time.Duration // attempt нумеруется с 1
}

type ConstantBackoff struct{ D time.Duration }

func NewExponentialBackoff(base, max time.Duration, factor int) (ExponentialBackoff, error)

type RetryConfig struct {
	MaxAttempts int
	Backoff     Backoff
	Retryable   func(error) bool
}

func NewRetry(clock Clock, cfg RetryConfig) (*Retry, error)
func (r *Retry) Do(op func() error) error

type RetryError struct {
	Attempts int
	Last     error
}
```

`Retry` повторяет упавшую операцию до `MaxAttempts` раз, делая паузы по `Backoff` (через `Clock`).
`ExponentialBackoff` растёт как `base * factor^(attempt-1)`, но не больше `max` (`max == 0` значит «без ограничения»).
`Retryable` решает, стоит ли повторять ошибку.

Если все попытки провалились, возвращается `*RetryError`. Его `Error()` выглядит как
`retries exhausted after 3 attempts: boom`, а `errors.Is` находит и `ErrRetriesExhausted`, и последнюю ошибку.

### 5. Jitter

```go
type Rand interface{ Float64() float64 } // число из [0, 1)

func NewFullJitter(inner Backoff, rnd Rand) (*FullJitter, error)
func NewEqualJitter(inner Backoff, rnd Rand) (*EqualJitter, error)
```

Если тысяча клиентов одновременно споткнулась и у всех одинаковая пауза, они одновременно же и вернутся
(«гром стада»). Случайность в паузе разносит их во времени. Оба типа оборачивают другой `Backoff`:
`FullJitter` даёт задержку из `[0, d)`, `EqualJitter` из `[d/2, d)`, где `d` это задержка внутреннего `Backoff`.

### 6. Limiter

```go
func NewLimiter[K comparable](clock Clock, capacity int, interval time.Duration) (*Limiter[K], error)
func (l *Limiter[K]) Allow(key K) bool
func (l *Limiter[K]) Do(key K, op func() error) error
func (l *Limiter[K]) Tokens(key K) int
func (l *Limiter[K]) Size() int
func (l *Limiter[K]) Sweep() int
func (l *Limiter[K]) Idle() iter.Seq[K]
```

Отдельный `TokenBucket` на каждый ключ (пользователя, IP, ...).

- `Size` возвращает число ключей, у которых есть ведро.
- `Sweep` освобождает память: удаляет ведра, которые уже ничем не отличаются от новых, и возвращает их число.
  Ответы лимитера от этого меняться не должны.
- `Idle` возвращает итератор по ключам, которые удалил бы `Sweep`.

### 7. Bulkhead

```go
func NewBulkhead(limit int) (*Bulkhead, error)
func (b *Bulkhead) Do(op func() error) error
func (b *Bulkhead) InFlight() int
```

Ограничивает число операций, которые выполняются **одновременно**. Если все `limit` слотов заняты,
`Do` возвращает `ErrBulkheadFull`. `InFlight` показывает, сколько операций выполняется сейчас.

### 8. Потокобезопасность

`TokenBucket`, `CircuitBreaker`, `Bulkhead`, `Limiter`, `FullJitter` и `EqualJitter` должны корректно работать
при вызовах из нескольких горутин.

<!-- HINTS_START -->

<!-- HINTS_END -->
