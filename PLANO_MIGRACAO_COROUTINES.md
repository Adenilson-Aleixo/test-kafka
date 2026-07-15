# Plano de Migração para Coroutines Avançadas no Consumidor Kafka

## Objetivo
Substituir o uso de `ExecutorService` e Threads bloqueantes por Kotlin Coroutines no `OrderKafkaConsumer`, visando maior eficiência, menor consumo de recursos e código mais idiomático.

## Etapas

### 1. Configuração do Escopo de Coroutines
- Criar um `CoroutineScope` personalizado com `SupervisorJob` e `Dispatchers.IO` (ideal para operações de I/O e Log).
- Garantir o cancelamento gracioso no `@PreDestroy`.

### 2. Refatoração para Structured Concurrency
O método `@KafkaListener` do Spring Kafka geralmente espera um retorno síncrono para confirmar o processamento (ack).
- Usaremos `runBlocking` no nível superior do listener para criar a ponte entre o mundo bloqueante do Kafka Listener e o mundo suspenso das Coroutines.
- Dentro do `runBlocking`, usaremos construtores de coroutines para processar as mensagens.

### 3. Implementação com Flow e Operadores (Avançado)
Em vez de um loop `for` simples, usaremos a API de `Flow` para processar o batch de mensagens de forma declarativa.

**Pipeline sugerido:**
1. **Converter**: `records.asFlow()`
2. **Parsing (JSON)**: Operador `.map`
3. **Filtragem (Idempotência)**: Operador `.filter` (suspendable se chamar banco)
4. **Processamento Concorrente**: Usar `flatMapMerge` ou `mapAsync` para processar múltiplas mensagens simultaneamente, respeitando um limite de concorrência.
5. **Tratamento de Erros**: Operador `.catch` ou `try/catch` dentro do fluxo para garantir que uma mensagem ruim não falhe o batch inteiro (Dead Letter Queue logic seria aplicada aqui).

## Exemplo do conceito final

```kotlin
@KafkaListener(...)
fun run(records: List<ConsumerRecord<String, String>>) = runBlocking(Dispatchers.Default) {
    records.asFlow()
        .map { /* Parse JSON */ }
        .filter { /* Check Idempotency */ }
        .flatMapMerge(concurrency = 10) { order ->
            flow {
                /* Process Business Logic */
                emit(order)
            }
        }
        .catch { e -> /* Log Error */ }
        .collect() // Aciona o fluxo
}
```

### 4. Otimizações Extras
- **Contexto de Log (MDC)**: Integrar `MDCContext()` para que os logs dentro das coroutines mantenham rastreabilidade.
- **Exception Handling**: Uso de `CoroutineExceptionHandler`.

## Próximos Passos
1. Criar o `CoroutineScope`.
2. Implementar a lógica com `Flow`.
3. Validar testes.

### Resumo simples (pra fixar)
- paralelismo
- resiliência
- controle de lifecycle
- 
## SupervisorJob()
- evita que uma falha derrube tudo
## CoroutineScope(...)
- onde suas coroutines vivem
## Dispatchers.IO
- executa em threads próprias para IO (Kafka, DB)