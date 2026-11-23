# What reading a prompt costs the answers being written beside it: time of one
# decoding step for `nseq` sequences, alone and with a chunk of another
# sequence's prompt in the same batch.
import llama_cpp as L, numpy as np, time, json, sys
L.llama_backend_init()
model = L.llama_model_load_from_file(sys.argv[1].encode(), L.llama_model_default_params())
vocab = L.llama_model_get_vocab(model)
nv = L.llama_vocab_n_tokens(vocab)
text = ("Write a long story about a river, a town and the people who live there. " * 400).encode()
toks = (L.llama_token * 8192)()
ntok = L.llama_tokenize(vocab, text, len(text), toks, 8192, True, False)
def run(nseq, chunk, steps=16, prompt_len=64):
    cp = L.llama_context_default_params()
    cp.n_ctx = max(prompt_len + steps + 8, chunk * steps + 64) * (nseq + 1); cp.n_batch = 4096; cp.n_seq_max = nseq + 1
    cp.n_threads = 4; cp.n_threads_batch = 4
    ctx = L.llama_init_from_model(model, cp)
    batch = L.llama_batch_init(4096, 0, 1)
    def fill(items):
        batch.n_tokens = len(items)
        for i, (tok, pos, seq, lg) in enumerate(items):
            batch.token[i] = tok; batch.pos[i] = pos; batch.n_seq_id[i] = 1; batch.seq_id[i][0] = seq; batch.logits[i] = lg
    for s in range(nseq):
        fill([(toks[i], i, s, i == prompt_len - 1) for i in range(prompt_len)])
        assert L.llama_decode(ctx, batch) == 0
    fill([(toks[prompt_len - 1], prompt_len, s, True) for s in range(nseq)])
    assert L.llama_decode(ctx, batch) == 0
    t0 = time.time()
    for k in range(steps):
        items = []
        for s in range(nseq):
            lg = L.llama_get_logits_ith(ctx, s)
            items.append((int(np.argmax(np.ctypeslib.as_array(lg, shape=(nv,)))), prompt_len + 1 + k, s, True))
        items += [(toks[k * chunk + i], k * chunk + i, nseq, False) for i in range(chunk)]
        fill(items)
        assert L.llama_decode(ctx, batch) == 0
    dt = (time.time() - t0) / steps
    L.llama_batch_free(batch); L.llama_free(ctx)
    return dt
run(2, 0, 4)
for nseq in [4, 8, 16]:
    base = run(nseq, 0)
    for chunk in [64, 256, 512]:
        t = run(nseq, chunk)
        print(json.dumps({"writing": nseq, "prompt_chunk_tokens": chunk, "step_ms_alone": round(base * 1000, 1), "step_ms_with_chunk": round(t * 1000, 1), "each_tokens_per_s_alone": round(1 / base, 1), "each_tokens_per_s_with_chunk": round(1 / t, 2), "chunk_read_tokens_per_s": round(chunk / (t - base), 0)}), flush=True)
