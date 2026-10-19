# Arquitetura

## Visão geral

Dois microsserviços independentes, cada um com seu banco. O `users-service` é a fonte da verdade sobre o usuário e seu plano. O `finance-service` mantém uma **cópia mínima** do usuário (`id`, `plan`, `trialEndsAt`) para aplicar regras de plano sem chamar o outro serviço.

A sincronização é feita por eventos Kafka. O consumer group configurado é `finance-service-group`; o Nest acrescenta `-server`, então no Kafka ele aparece como `finance-service-group-server`.

## Eventos Kafka

| Tópico | Produtor | Consumidor | Payload | Efeito |
|---|---|---|---|---|
| `user-created` | users-service (no cadastro) | finance-service | `{ id, plan, trialEndsAt }` | cria o usuário no banco do finance |
| `user-plan-updated` | users-service (compra validada ou assinatura cancelada) | finance-service | `{ id, plan }` | atualiza o plano do usuário |
| `user-deleted` | users-service (exclusão de conta) | finance-service | `{ id }` | apaga despesas, receitas, categorias e o usuário, em transação (idempotente) |

O finance-service cria os três tópicos ao iniciar, para que o consumer nunca assine um tópico inexistente.

### Como o finance-service consome os eventos

- **Idempotente.** `user-created` é um `upsert`: reentregas não falham e não sobrescrevem o plano.
- **Independente da ordem.** Tópicos diferentes não têm ordem garantida entre si. `user-plan-updated` também é um `upsert` (cria o usuário com `trialEndsAt` provisório se ele ainda não existir) e `user-created` só preenche o `trialEndsAt` real, sem mexer no plano. Qualquer ordem chega ao mesmo estado.
- **Retry e DLQ.** O erro é classificado antes de tentar de novo (`is-transient-error.ts`):
  - **Permanente** (payload inválido, violação de constraint, `TypeError`): vai **direto** para a DLQ, sem retry e sem espera.
  - **Passageiro** (banco inacessível, timeout, conexão derrubada e erros desconhecidos): até 3 tentativas, com espera de 0,5 s e depois 1 s (no máximo 1,5 s de bloqueio). Na dúvida, o erro é tratado como passageiro.
  - Esgotadas as tentativas, a mensagem vai para `<tópico>.dlq` (`user-created.dlq`, `user-plan-updated.dlq`, `user-deleted.dlq`) com o payload original, o erro e a data, e o consumer segue em frente em vez de travar a partição. Se nem a DLQ aceitar, o erro é relançado e o Kafka reentrega.
  - A DLQ não é reprocessada sozinha nem gera alerta, e o Kafka apaga mensagens após 7 dias por padrão. O reprocessamento é manual: ler a DLQ, corrigir a causa e republicar o `payload` no tópico original.
- **`fromBeginning: true`.** Um grupo sem offset (primeira execução, Kafka recriado) lê o histórico em vez de ignorá-lo; depois que o grupo tem offset, um restart continua de onde parou.

Limitação conhecida: ao reler o histórico, um `user-created` de um usuário já excluído pode ser processado depois do seu `user-deleted`, deixando um registro de usuário sem dados no finance. É inofensivo; a solução definitiva seria um único tópico `user-events` com chave igual ao id do usuário.

## Camadas dos serviços (Clean Architecture)

```
src/
├── presentation/controllers/   endpoints REST
├── application/
│   ├── auth/                   AuthGuard (JWT)
│   └── use-cases/              uma classe por operação
├── domain/
│   ├── DTO/                    entrada/saída + class-validator
│   ├── entities/
│   └── use-cases-protocol/     contratos (tokens de injeção)
├── infra/
│   ├── database/               Prisma + repositórios
│   ├── kafka/                  producers (users) / consumers (finance)
│   └── store-api/              Google Play (users)
└── shared/utils/
```

Controllers dependem dos *protocols*; o Nest injeta a implementação concreta em cada módulo.

## Modelo de dados

**users-service** — `User(id, name, email unique, password, plan, created_at, trialStartedAt, trialEndsAt, subscriptionPurchaseToken?, subscriptionExpiresAt?)`

**finance-service**

- `User(id, plan, trialEndsAt)`
- `Category(id, user_id?, type INCOME|EXPENSE, name, icon, created_at, archived_at?)` — `user_id = null` são as categorias padrão do seed. Excluir uma categoria só preenche `archived_at` (soft delete): o histórico de transações e séries recorrentes que a usam fica intacto
- `Expense` e `Income`: `amount (Decimal)`, `category_id`, `description?`, `recurrence NONE|MONTHLY|YEARLY`, `total_installments`, `current_installment`, `installment_group_id?`, `recurrence_series_id?`, `transaction_date`, `created_at`
- `RecurringSeries(id, user_id, kind EXPENSE|INCOME, frequency MONTHLY|YEARLY, anchor_date, category_id, amount, description?, generated_until, active)`: o molde de uma transação recorrente. Índice único `(recurrence_series_id, transaction_date)` em despesas e receitas.

Enum `Plan`: `FREE | TRIAL | PREMIUM`.

## Regras de plano

- Todo usuário nasce com `plan = FREE` e `trialEndsAt = agora + 7 dias`.
- Enquanto `trialEndsAt` não passou, o usuário tem acesso completo. Quem decide é o **servidor**: `GET /users` devolve `isPremium` (Premium ou trial ativo) e `isTrialActive`, e o app só os usa. A mesma regra (`hasPremiumAccess`) existe nos dois serviços.
- Com `FREE` e trial expirado a conta fica **somente leitura**: o finance-service bloqueia **criar e editar** categorias, despesas e receitas (`403`). **Excluir continua permitido**, para o usuário não ficar refém dos próprios dados.
- O valor `TRIAL` do enum `Plan` existe mas não é atribuído: o trial é derivado (`FREE` + `trialEndsAt` no futuro).
- Compra: o app envia `purchaseToken` + `productId` para `POST /subscriptions/validate`; o servidor consulta a Google Play API, grava `PREMIUM` e publica `user-plan-updated`.
- Cancelamento/expiração: a Google envia notificação Pub/Sub para `POST /subscriptions/webhook/google-play` (autenticada por token Bearer); os tipos 3, 12 e 13 voltam o usuário para `FREE`.

## Segurança HTTP

- **401** = token ausente, inválido ou expirado. O app encerra a sessão ao receber 401 em uma chamada autenticada.
- **403** = autenticado, mas sem permissão (senha atual incorreta, tentar alterar outro usuário, mexer em registro de outro dono). Não encerra a sessão.
- **Rate limit atrás do proxy:** os serviços usam `trust proxy` (`TRUST_PROXY_HOPS`, padrão `1` = um proxy reverso na frente) para o `req.ip` ser o IP real do cliente; sem isso todos os usuários dividiriam o mesmo limite.

## Exclusão de conta

`DELETE /users/:id` exige a senha. O users-service remove o usuário e publica `user-deleted`; se a publicação falhar, o erro é registrado em log (a conta já foi removida) e os dados do finance precisam ser limpos manualmente. A assinatura Google Play **não** é cancelada pelo servidor: o app avisa o usuário para cancelar na Play Store.

## Datas

A data de uma transação é um **dia de calendário**, não um instante. O app envia `AAAA-MM-DD` (o dia local escolhido) e o servidor grava às **12:00 UTC** desse dia. O meio-dia mantém o mesmo dia em getters locais e UTC para qualquer fuso de UTC-11 a UTC+11, então não há conta de fuso em lugar nenhum. O filtro do mês (`start_date`/`end_date`) usa o mesmo formato. Formatos com horário são rejeitados (`400`).

`created_at` é o instante real em UTC, definido pelo banco.

No front, `utils/date-only.ts` concentra a leitura/escrita desses dias; evite `toISOString()` e getters locais em datas vindas do servidor.

## Parcelamento

Ao criar com `total_installments > 1` (máximo 60), o `amount` enviado é o **valor total** da compra. O serviço (`shared/transactions/installments.ts`):

- divide em **centavos inteiros**; os centavos que sobram vão 1 a 1 para as primeiras parcelas, então a diferença entre parcelas nunca passa de 1 centavo (R$ 100 em 3x = 33,34 + 33,33 + 33,33). Valor baixo demais para o número de parcelas (menos de R$ 0,01 por parcela) é rejeitado;
- calcula cada data somando `i` meses à data **original**, ajustando ao último dia do mês (31/jan: 31/01, 28/02, 31/03...);
- grava todas as parcelas numa **única transação** do banco (tudo ou nada), com o mesmo `installment_group_id`.

`applyToAll` na edição/exclusão atua no grupo todo. Não é permitido combinar parcelamento com recorrência nem mudar a data de todas as parcelas de uma vez.

## Recorrência

Uma despesa ou receita `MONTHLY`/`YEARLY` cria uma `RecurringSeries` e a 1ª ocorrência (na mesma transação). As demais são **geradas sob demanda**: ao consultar `GET /transactions` com período, o servidor cria as ocorrências que faltam até o fim do período, limitado a **24 meses à frente de hoje** (`RECURRENCE_HORIZON_MONTHS`). O limite acompanha a data atual, então a série nunca "acaba": conforme o tempo passa, novas ocorrências continuam sendo geradas.

- Cada ocorrência é uma linha normal (editável e excluível). As datas saem sempre da data âncora, com ajuste ao fim do mês (mensal 31/jan: 28/fev, 31/mar, 30/abr; anual 29/fev: 28/fev em ano não bissexto).
- A geração é idempotente: só cria datas depois de `generated_until` e o índice único ignora duplicatas, então requisições simultâneas não duplicam. Excluir uma ocorrência não a faz voltar.
- **Editar** com `applyToAll` = "esta e as seguintes": altera as ocorrências daquela data em diante e o molde da série (as próximas já saem com o valor novo). Só valor, categoria e descrição; data, recorrência e parcelas não mudam em lote.
- **Excluir** com `applyToAll` = "esta e as seguintes": remove daquela data em diante e encerra a série (`active = false`); se for a 1ª ocorrência, remove a série inteira. Sem `applyToAll`, remove só aquela ocorrência.
- A geração continua para quem está em plano somente leitura: são dados do próprio usuário.
- A recorrência de uma transação existente não pode ser alterada.

## Front-end

- `App.tsx`: `withIAPContext` → `AuthProvider` → `TransactionsProvider` → navegação.
- `routes/Stack.tsx` escolhe `PublicRoutes` (Login/SignUp) ou `ProtectedRoutes` conforme o token.
- Estado global só com Context: `AuthContext` (token em AsyncStorage `@money-track-token!`, usuário, `isPremium`) e `TransactionsContext` (transações do mês + flag `shouldRefetch`).
- `hooks/` faz as chamadas; `services/` concentra as chamadas Axios. O cache de transações (AsyncStorage, por mês) guarda a hora da gravação: o app mostra o cache na hora e, se tiver mais de 60 s, **revalida em segundo plano**. Somas de valores usam centavos inteiros (`utils/money.ts`); URLs vêm do `.env` via `react-native-dotenv` (`@env`).
