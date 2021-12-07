# ejecutar en lugar de main
import pandas as pd
directorio = r"C:\Users\opaucarb\Documents\AVANCE_TRANSFORMACION_DIGITAL\ARCHIVOS\JUEZ_ESCUCHA\T-224479-Ejecutar el script adjunto en la bds Juez te Escucha - GG.xlsx"
df = pd.read_excel(directorio)

df['F_ATENCION_INICIO2'] = df['F_ATENCION_INICIO'].apply(lambda x: x[:10])

fecha_list=df['F_ATENCION_INICIO2'].unique().tolist()
fecha_list2 = [item.replace("/", "-") for item in fecha_list]
i=0
for n in fecha_list:
    df1 = df[df['F_ATENCION_INICIO2'] == fecha_list[0]]
    df1.to_excel('juez_escucha_'+fecha_list2[i]+'.xlsx')
    i=i+1
