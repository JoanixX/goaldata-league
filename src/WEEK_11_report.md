# Reporte Técnico de Evaluación e Ingeniería de Decisiones (Week 10)

### 1. Candidate-Pool Definition (Fronteras Operativas)
En conformidad con los estándares de control de escasez (sparsity) estudiados en la teoría de Collaborative Filtering, el universo completo de 4,065 perfiles se somete a una restricción de vecindario dura. El **Candidate Pool** se delimita exclusivamente por el identificador de `kmeans_cluster`. Esto mitiga el costo computacional de evaluar similitudes cruzadas absurdas (vanguardias contra retaguardias).

### 2. Protocolo de Evaluación e Inferencia del Sistema
Para validar de manera científica que el Motor Avanzado (Hybrid Ranking) no depende del sesgo de popularidad, el protocolo contrasta el rendimiento del cálculo angular en espacio latente frente a un clasificador de frecuencia pura (Baseline de Popularidad). Las métricas utilizadas son:
* **Precision@5:** Evalúa la densidad de aciertos de posición táctica en la ventana de elecciones del Director Deportivo.
* **Recall@5:** Mide la tasa de cobertura o probabilidad de recuperar al menos una alternativa funcional exacta dentro del Top 5.
* **Catalog Coverage:** Monitorea el nivel de dispersión o diversidad de las sugerencias para evitar el fenómeno de estancamiento de catálogo (*filter bubble*).

### 3. Justificación de Métricas Obtenidas
El sistema avanzado demuestra una ventaja significativa en **Catalog Coverage** frente al baseline. Mientras que el modelo de popularidad tiende a sobre-recomendar de forma repetitiva a los mismos elementos con mayor cantidad de registros históricos, el Motor Híbrido aprovecha las coordenadas latentes individuales de los jugadores menos conocidos, distribuyendo las recomendaciones en una porción de mercado sustancialmente más amplia sin degradar la precisión posicional.