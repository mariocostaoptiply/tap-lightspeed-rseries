#!/bin/bash
# Script para verificar se o replication_key está a funcionar corretamente

set -e

echo "=========================================="
echo "Verificação do Replication Key (version)"
echo "=========================================="
echo ""

# Cores para output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 1. Primeira execução - verificar se obtém dados e emite STATE
echo -e "${YELLOW}1. Primeira execução (sem state)${NC}"
echo "Executando tap e guardando output em /tmp/tap_output_1.jsonl..."
echo ""

tap-x-lightspeed --config config.json --catalog catalog.json 2>&1 | tee /tmp/tap_output_1.jsonl

echo ""
echo -e "${GREEN}✓ Primeira execução concluída${NC}"
echo ""

# 2. Extrair mensagens STATE
echo -e "${YELLOW}2. Extraindo mensagens STATE da primeira execução${NC}"
echo ""

STATE_1=$(grep '"type": "STATE"' /tmp/tap_output_1.jsonl | tail -1)
if [ -z "$STATE_1" ]; then
    echo "⚠ Nenhuma mensagem STATE encontrada na primeira execução"
else
    echo "Última mensagem STATE:"
    echo "$STATE_1" | python3 -m json.tool 2>/dev/null || echo "$STATE_1"
    
    # Extrair o version máximo
    VERSION_1=$(echo "$STATE_1" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('value', {}).get('bookmarks', {}).get('products', {}).get('replication_key_value', 'N/A'))" 2>/dev/null || echo "N/A")
    echo ""
    echo "Version guardado no state: $VERSION_1"
fi

echo ""
echo "=========================================="
echo ""

# 3. Segunda execução - verificar se usa o state anterior
echo -e "${YELLOW}3. Segunda execução (com state)${NC}"
echo "Executando tap novamente para verificar se usa o state anterior..."
echo ""

# Guardar state da primeira execução
if [ ! -z "$STATE_1" ]; then
    echo "$STATE_1" | python3 -c "import sys, json; data=json.load(sys.stdin); print(json.dumps(data.get('value', {})))" > /tmp/state.json 2>/dev/null || echo "{}" > /tmp/state.json
    echo "State guardado em /tmp/state.json"
    echo ""
fi

# Executar segunda vez com state
tap-x-lightspeed --config config.json --catalog catalog.json --state /tmp/state.json 2>&1 | tee /tmp/tap_output_2.jsonl

echo ""
echo -e "${GREEN}✓ Segunda execução concluída${NC}"
echo ""

# 4. Comparar resultados
echo -e "${YELLOW}4. Comparando resultados${NC}"
echo ""

RECORDS_1=$(grep '"type": "RECORD"' /tmp/tap_output_1.jsonl 2>/dev/null | wc -l | tr -d ' ')
RECORDS_2=$(grep '"type": "RECORD"' /tmp/tap_output_2.jsonl 2>/dev/null | wc -l | tr -d ' ')

echo "Records na primeira execução: $RECORDS_1"
echo "Records na segunda execução: $RECORDS_2"

if [ "$RECORDS_2" -lt "$RECORDS_1" ]; then
    echo -e "${GREEN}✓ Replication key está a funcionar!${NC}"
    echo "   A segunda execução obteve menos records, o que indica que está a usar o state."
else
    echo -e "${YELLOW}⚠ A segunda execução obteve o mesmo número ou mais records${NC}"
    echo "   Isto pode indicar que o replication key não está a funcionar corretamente."
fi

echo ""
echo "=========================================="
echo ""

# 5. Verificar logs para ver qual version está a ser usado
echo -e "${YELLOW}5. Verificando logs para ver qual 'after' parameter está a ser usado${NC}"
echo ""

echo "Procurando por 'after' nos logs da segunda execução..."
grep -i "after" /tmp/tap_output_2.jsonl | head -5 || echo "Não encontrado nos logs (pode estar no nível DEBUG)"

echo ""
echo "=========================================="
echo ""
echo "Para ver mais detalhes, pode:"
echo "  1. Ver o state completo: cat /tmp/state.json | python3 -m json.tool"
echo "  2. Ver todos os records: grep '\"type\": \"RECORD\"' /tmp/tap_output_2.jsonl"
echo "  3. Verificar versões nos records: grep '\"version\"' /tmp/tap_output_2.jsonl | head -5"

