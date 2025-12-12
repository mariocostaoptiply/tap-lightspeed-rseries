#!/bin/bash
# Versão simplificada para verificar replication key rapidamente

echo "=== Verificação Rápida do Replication Key ==="
echo ""

# Executar tap e filtrar apenas STATE messages
echo "Executando tap e extraindo mensagens STATE..."
echo ""

tap-x-lightspeed --config config.json --catalog catalog.json 2>&1 | \
    grep '"type": "STATE"' | \
    tail -1 | \
    python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    state = data.get('value', {}).get('bookmarks', {}).get('products', {})
    version = state.get('replication_key_value', 'N/A')
    print(f'✓ Version guardado no state: {version}')
    print(f'  State completo: {json.dumps(state, indent=2)}')
except Exception as e:
    print(f'Erro ao processar state: {e}')
    print(sys.stdin.read())
" 2>/dev/null || echo "Nenhuma mensagem STATE encontrada"

echo ""
echo "Para ver todos os records com version:"
echo "  tap-x-lightspeed --config config.json --catalog catalog.json | grep '\"version\"' | head -10"

