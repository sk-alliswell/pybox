# Batched decoding on one model: output tokens a second in total and per sequence
# as the number of sequences decoded together grows.
import llama_cpp as L, numpy as np, time, json, ctypes, sys
L.llama_backend_init()
mp = L.llama_model_default_params()
model = L.llama_model_load_from_file(sys.argv[1].encode(), mp)
vocab = L.llama_model_get_vocab(model)
nv = L.llama_vocab_n_tokens(vocab)
def run(nseq, steps=48, prompt_len=64):
    cp = L.llama_context_default_params()
    cp.n_ctx = (prompt_len + steps + 8) * nseq; cp.n_batch = 2048; cp.n_seq_max = nseq
    cp.n_threads = 4; cp.n_threads_batch = 4
    ctx = L.llama_init_from_model(model, cp)
    text = ("Write a long story about a river, a town and the people who live there. " * 8).encode()
    toks = (L.llama_token * 512)()
    n = L.llama_tokenize(vocab, text, len(text), toks, 512, True, False)
    n = min(n, prompt_len)
    batch = L.llama_batch_init(2048, 0, 1)
    def fill(items):
        batch.n_tokens = len(items)
        for i, (tok, pos, seq, lg) in enumerate(items):
            batch.token[i] = tok; batch.pos[i] = pos; batch.n_seq_id[i] = 1; batch.seq_id[i][0] = seq; batch.logits[i] = lg
    t0 = time.time()
    for s in range(nseq):
        fill([(toks[i], i, s, i == n - 1) for i in range(n)])
        assert L.llama_decode(ctx, batch) == 0
    pre = time.time() - t0
    last = [toks[n - 1]] * nseq
    # first sampled tokens: just take argmax of each sequence's last logits
    idx = [n - 1] * nseq  # index in last batch only valid for last seq; resample by a first joint step
    fill([(toks[n - 1], n, s, True) for s in range(nseq)])
    assert L.llama_decode(ctx, batch) == 0
    t1 = time.time()
    for k in range(steps):
        nxt = []
        for s in range(nseq):
            lg = L.llama_get_logits_ith(ctx, s)
            nxt.append(int(np.argmax(np.ctypeslib.as_array(lg, shape=(nv,)))))
        fill([(nxt[s], n + 1 + k, s, True) for s in range(nseq)])
        assert L.llama_decode(ctx, batch) == 0
    dt = time.time() - t1
    L.llama_batch_free(batch); L.llama_free(ctx)
    return {"together": nseq, "total_tokens_per_s": round(nseq * steps / dt, 1), "each_tokens_per_s": round(steps / dt, 2), "prompt_tokens_per_s": round(nseq * n / pre, 0)}
run(1, 8)
for nseq in [1, 2, 4, 8, 16, 32, 64]:
    print(json.dumps(run(nseq)), flush=True)
